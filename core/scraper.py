# core/scraper.py
"""
core/scraper.py — Orchestration: fetch → parse → lưu → điều hướng.

Fixes applied:
  FIX-A: Cache `ai_classify_and_find` result — tránh gọi 2 lần cùng payload
          (lần 1 cho content extraction, lần 2 cho next_url).
  FIX-B: Xóa `ask_ai_confirm_same_story` khỏi vòng lặp per-chapter.
          Guard này broken (title2="") và redundant khi story_id đã lock.
          Chỉ giữ lại khi domain thay đổi đột ngột (cross-domain jump).
  FIX-C: `_extract_content_ai` được inline vào `scrape_one_chapter`
          để có thể chia sẻ biến cache — hàm cũ bị xóa.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import (
    CONTENT_SELECTORS,
    MAX_CHAPTERS,
    MAX_CONSECUTIVE_ERRORS,
    MAX_CONSECUTIVE_TIMEOUTS,
    TIMEOUT_BACKOFF_BASE,
    STORY_ID_LEARN_AFTER,
    STORY_ID_MAX_ATTEMPTS,
    get_delay_seconds,
)
from utils.file_io import load_progress, save_progress, write_markdown, save_profiles
from utils.string_helpers import (
    is_junk_page, make_fingerprint, clean_chapter_text,
    normalize_title, slugify_filename, truncate,
    extract_text_blocks,
)
from utils.types import AiClassifyResult, ProgressDict, SiteProfileDict, StoryIdResult
from ai.client  import AIRateLimiter
from ai.agents  import (
    ask_ai_for_story_id,
    ai_find_first_chapter_url,
    ai_classify_and_find,
    ask_ai_build_profile,
    ask_ai_confirm_same_story,
)
from core.fetch       import fetch_page
from core.navigator   import find_next_url, detect_page_type
from core.html_filter import remove_hidden_elements
from core.extractors  import TitleExtractor, extract_story_title
from core.session_pool import DomainSessionPool, PlaywrightPool

logger = logging.getLogger(__name__)

_COLLECTED_URL_CAP = 20


# ── CPU-bound helpers ─────────────────────────────────────────────────────────

def _sync_parse_and_clean(html: str) -> tuple[BeautifulSoup, str]:
    soup = BeautifulSoup(html, "html.parser")
    remove_hidden_elements(soup)
    return soup, str(soup)


def _sync_detect_page_type(html: str, url: str) -> str:
    """Wrapper sync cho detect_page_type — parse html → soup rồi gọi."""
    soup = BeautifulSoup(html, "html.parser")
    return detect_page_type(soup, url)


def _sync_extract_content(soup: BeautifulSoup) -> str | None:
    for sel in CONTENT_SELECTORS:
        el = soup.select_one(sel)
        if el:
            text = extract_text_blocks(el)
            if len(text.strip()) > 200:
                return text
    return None


# ── Profile management ────────────────────────────────────────────────────────

async def _save_new_profile(
    profiles: dict[str, SiteProfileDict],
    domain: str,
    new_profile: SiteProfileDict,
    profiles_lock: asyncio.Lock,
) -> None:
    async with profiles_lock:
        profiles[domain] = new_profile
        await save_profiles(profiles)


# ── Story ID guard ────────────────────────────────────────────────────────────

def _check_story_id_guard(url: str, progress: ProgressDict) -> bool:
    if not progress.get("story_id_locked"):
        return True
    pattern = progress.get("story_id_regex")
    if not pattern:
        return True
    try:
        return bool(re.search(pattern, url))
    except re.error:
        return True


# ── Find start chapter ────────────────────────────────────────────────────────

async def check_and_find_start_chapter(
    start_url: str,
    progress_path: str,
    pool: DomainSessionPool,
    pw_pool: PlaywrightPool,
    profiles: dict[str, SiteProfileDict],
    ai_limiter: AIRateLimiter,
) -> tuple[str, ProgressDict]:
    """
    Xác định URL chương đầu tiên cần cào.

    - Resume: trả về current_url đã lưu trong progress
    - Index page: nhờ AI tìm chương đầu
    - Chapter page: trả về start_url thẳng

    FIX: Sau khi detect "chapter", kiểm tra nhanh có nội dung không.
    Nếu không có (ví dụ: RoyalRoad fiction index bị phân loại sai do JS),
    fallback sang index logic để tìm chương đầu.
    """
    progress = await load_progress(progress_path)

    if progress.get("current_url"):
        print(f"  [Resume] ▶ Tiếp tục từ: {progress['current_url'][:70]}", flush=True)
        return progress["current_url"], progress  # type: ignore[return-value]

    if progress.get("completed"):
        n = progress.get("chapter_count", 0)
        print(f"  [Done] ✔ Truyện đã cào xong ({n} chương).", flush=True)
        raise RuntimeError("Truyện đã hoàn thành, bỏ qua.")

    status, html = await fetch_page(start_url, pool, pw_pool)
    if status not in (200, 206):
        raise RuntimeError(f"HTTP {status}: {start_url}")
    if is_junk_page(html, status):
        raise RuntimeError(f"Trang khởi đầu trả về lỗi/rỗng: {start_url}")

    page_type = await asyncio.to_thread(_sync_detect_page_type, html, start_url)

    if page_type == "chapter":
        # Xác minh: thực sự có nội dung chương không?
        soup_check, _ = await asyncio.to_thread(_sync_parse_and_clean, html)
        content_check = await asyncio.to_thread(_sync_extract_content, soup_check)

        if content_check and len(content_check.strip()) > 200:
            print(f"  [Start] 📖 Bắt đầu từ chương: {start_url[:70]}", flush=True)
            return start_url, progress

        print(
            f"  [Start] 🔄 Phát hiện là chương nhưng không có nội dung"
            f" → thử tìm chương đầu...",
            flush=True,
        )
        page_type = "index"

    print(f"  [Start] 📋 Trang index, tìm chương đầu...", flush=True)
    first_url = await ai_find_first_chapter_url(html, start_url, ai_limiter)
    if first_url:
        print(f"  [Start] ✅ Chương đầu: {first_url[:70]}", flush=True)
        return first_url, progress

    print(f"  [Start] 🤖 Nhờ AI phân tích trang...", flush=True)
    result: AiClassifyResult | None = await ai_classify_and_find(html, start_url, ai_limiter)
    if result:
        if result.get("page_type") == "chapter":
            print(f"  [Start] 📖 AI xác nhận: đây là trang chương.", flush=True)
            return start_url, progress
        for key in ("first_chapter_url", "next_url"):
            found = result.get(key)  # type: ignore[literal-required]
            if found:
                print(f"  [Start] ✅ AI tìm được URL: {found[:70]}", flush=True)
                return found, progress

    raise RuntimeError(f"Không tìm được điểm bắt đầu từ: {start_url}")


# ── Scrape one chapter ────────────────────────────────────────────────────────

async def scrape_one_chapter(
    url: str,
    progress: ProgressDict,
    progress_path: str,
    output_dir: str,
    pool: DomainSessionPool,
    pw_pool: PlaywrightPool,
    profiles: dict[str, SiteProfileDict],
    profiles_lock: asyncio.Lock,
    ai_limiter: AIRateLimiter,
    title_extractor: TitleExtractor,
) -> str | None:
    all_visited: set[str] = set(progress.get("all_visited_urls") or [])

    if url in all_visited:
        return await _advance_past_visited(url, all_visited, progress, progress_path,
                                           pool, pw_pool, profiles, ai_limiter)

    status, html = await fetch_page(url, pool, pw_pool)
    if is_junk_page(html, status):
        print(f"  [End] 🏁 Hết truyện hoặc trang lỗi: {url[:60]}", flush=True)
        return None

    # CPU-bound: parse + clean
    soup, clean_html = await asyncio.to_thread(_sync_parse_and_clean, html)

    domain  = urlparse(url).netloc.lower()

    # Profile
    profile: SiteProfileDict = profiles.get(domain, {})
    if not profile:
        new_profile = await ask_ai_build_profile(clean_html, url, ai_limiter)
        if new_profile:
            await _save_new_profile(profiles, domain, new_profile, profiles_lock)
            profile = new_profile
            print(f"  [Profile] ✅ Đã lưu profile cho {domain}", flush=True)

    # ── Extract nội dung ──────────────────────────────────────────────────────
    # FIX-A: Cache ai_classify_and_find result.
    # Nếu CSS selectors không tìm được content, gọi AI 1 lần và giữ cache.
    # Khi cần tìm next_url sau đó, dùng lại cache — không gọi AI lần 2.
    ai_classify_cache: AiClassifyResult | None = None

    content = await asyncio.to_thread(_sync_extract_content, soup)

    if content is None:
        # Gọi AI lần duy nhất — lấy cả page_type lẫn next_url
        ai_classify_cache = await ai_classify_and_find(clean_html, url, ai_limiter)
        if ai_classify_cache and ai_classify_cache.get("page_type") == "chapter":
            body = soup.find("body")
            if body:
                content = extract_text_blocks(body)

    if not content or len(content.strip()) < 100:
        print(f"  [Skip] Không trích được nội dung: {url[:60]}", flush=True)
        return None

    content = clean_chapter_text(content)

    # Fingerprint
    fp           = make_fingerprint(content)
    fingerprints = set(progress.get("fingerprints") or [])
    if fp in fingerprints:
        print(f"  [Loop] ♻ Nội dung lặp lại: {url[:60]}", flush=True)
        return None
    fingerprints.add(fp)
    progress["fingerprints"] = list(fingerprints)

    # FIX-D: truyền ai_limiter để dùng AI khi vote hòa (thay vì max-len fallback)
    title = normalize_title(await title_extractor.extract(soup, url, ai_limiter))

    # Lưu tên truyện
    if progress.get("chapter_count", 0) == 0 and not progress.get("story_title"):
        story_title = extract_story_title(soup, url)
        if story_title:
            progress["story_title"] = story_title

    # Ghi file .md
    chapter_num  = progress.get("chapter_count", 0) + 1
    filename     = f"{chapter_num:04d}_{slugify_filename(title, max_len=60)}.md"
    file_content = f"# {title}\n\n{content}\n"
    await write_markdown(os.path.join(output_dir, filename), file_content)

    # Cập nhật progress
    progress["chapter_count"]    = chapter_num
    progress["last_title"]       = title
    progress["last_scraped_url"] = url

    all_visited.add(url)
    progress["all_visited_urls"] = list(all_visited)

    if not progress.get("story_id_locked"):
        collected: list[str] = progress.get("collected_urls") or []
        if url not in collected:
            collected.append(url)
        progress["collected_urls"] = collected[-_COLLECTED_URL_CAP:]

        if (
            len(progress["collected_urls"]) >= STORY_ID_LEARN_AFTER
            and progress.get("story_id_attempts", 0) < STORY_ID_MAX_ATTEMPTS
        ):
            result: StoryIdResult | None = await ask_ai_for_story_id(
                progress["collected_urls"], ai_limiter
            )
            if result:
                progress["story_id"]        = result.get("story_id")
                progress["story_id_regex"]  = result.get("story_id_regex")
                progress["story_id_locked"] = True
                print(f"  [Guard] 🔐 Story ID: {result.get('story_id')}", flush=True)
            else:
                progress["story_id_attempts"] = progress.get("story_id_attempts", 0) + 1

    print(
        f"  ✅ Ch.{chapter_num:>4}: {truncate(title, 45):<45}"
        f" | {len(content):>5} ký tự",
        flush=True,
    )

    # ── Tìm next_url ─────────────────────────────────────────────────────────
    # FIX-A (tiếp): Nếu ai_classify_cache đã có (từ content extraction),
    # dùng lại thay vì gọi AI lần 2.
    next_url = find_next_url(soup, url, profile)
    if not next_url:
        if ai_classify_cache is None:
            # Chỉ gọi khi chưa có cache (tức CSS đã tìm được content)
            ai_classify_cache = await ai_classify_and_find(clean_html, url, ai_limiter)
        if ai_classify_cache:
            next_url = ai_classify_cache.get("next_url")

    if not next_url:
        progress["completed"]        = True
        progress["completed_at_url"] = url
        await save_progress(progress_path, progress)
        print(f"  [End] 🏁 Hết truyện.", flush=True)
        return None

    if not _check_story_id_guard(next_url, progress):
        print(f"  [Guard] ⛔ URL lạ bị chặn: {next_url[:60]}", flush=True)
        return None

    if next_url in all_visited:
        print(f"  [Loop] ♻ URL đã thăm: {next_url[:60]}", flush=True)
        return None

    # ── FIX-B: Cross-domain jump guard ───────────────────────────────────────
    # Thay thế ask_ai_confirm_same_story gọi mỗi chương (broken + expensive)
    # bằng kiểm tra domain đơn giản + chỉ gọi AI khi domain thực sự thay đổi.
    #
    # Lý do xóa vòng lặp cũ:
    #   1. title2="" → AI không đủ thông tin → luôn trả về same_story=True
    #   2. story_id guard đã xử lý sau ch.12, tốn thêm 1 RPM/ch là lãng phí
    #   3. Trường hợp thực sự cần: URL nhảy sang domain khác hẳn
    current_domain = urlparse(url).netloc
    next_domain    = urlparse(next_url).netloc

    if (
        not progress.get("story_id_locked")
        and next_domain != current_domain
    ):
        # Domain thay đổi: gọi AI với title hiện tại (title2 vẫn rỗng vì chưa fetch)
        # Ở đây ta chỉ cần check URL pattern, không cần title2
        print(
            f"  [Guard] ⚠️ Domain thay đổi: {current_domain} → {next_domain}",
            flush=True,
        )
        is_same = await ask_ai_confirm_same_story(
            title1     = title,
            url1       = url,
            title2     = "",        # chưa fetch trang tiếp → không có title
            url2       = next_url,
            ai_limiter = ai_limiter,
        )
        if not is_same:
            print(
                f"  [Guard] ⛔ Next URL có vẻ thuộc truyện khác: {next_url[:60]}",
                flush=True,
            )
            progress["completed"]        = True
            progress["completed_at_url"] = url
            await save_progress(progress_path, progress)
            return None

    progress["current_url"] = next_url
    await save_progress(progress_path, progress)
    return next_url


# ── Run novel task ────────────────────────────────────────────────────────────

async def run_novel_task(
    start_url: str,
    output_dir: str,
    progress_path: str,
    pool: DomainSessionPool,
    pw_pool: PlaywrightPool,
    profiles: dict[str, SiteProfileDict],
    profiles_lock: asyncio.Lock,
    ai_limiter: AIRateLimiter,
    on_chapter_done=None,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    title_extractor      = TitleExtractor()
    consecutive_errors   = 0
    consecutive_timeouts = 0

    try:
        current_url, progress = await check_and_find_start_chapter(
            start_url, progress_path, pool, pw_pool, profiles, ai_limiter,
        )
    except Exception as e:
        print(f"  [ERR] Không tìm được điểm bắt đầu: {e}", flush=True)
        return

    print(f"\n🚀 Bắt đầu: {progress.get('story_title') or start_url[:50]}", flush=True)

    while current_url and progress.get("chapter_count", 0) < MAX_CHAPTERS:
        if progress.get("completed"):
            break

        await asyncio.sleep(get_delay_seconds(current_url))

        try:
            prev_count = progress.get("chapter_count", 0)
            next_url   = await scrape_one_chapter(
                url             = current_url,
                progress        = progress,
                progress_path   = progress_path,
                output_dir      = output_dir,
                pool            = pool,
                pw_pool         = pw_pool,
                profiles        = profiles,
                profiles_lock   = profiles_lock,
                ai_limiter      = ai_limiter,
                title_extractor = title_extractor,
            )
            consecutive_errors   = 0
            consecutive_timeouts = 0

            if on_chapter_done and progress.get("chapter_count", 0) > prev_count:
                await on_chapter_done()

            current_url = next_url

        except asyncio.TimeoutError:
            consecutive_timeouts += 1
            wait = TIMEOUT_BACKOFF_BASE * consecutive_timeouts
            print(f"  [Timeout #{consecutive_timeouts}] Chờ {wait}s", flush=True)
            if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                print(f"  [ERR] Quá nhiều timeout. Dừng.", flush=True)
                break
            await asyncio.sleep(wait)

        except Exception as e:
            consecutive_errors += 1
            print(f"  [ERR #{consecutive_errors}] {type(e).__name__}: {e}", flush=True)
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print(f"  [ERR] Quá nhiều lỗi liên tiếp. Dừng.", flush=True)
                break

    total     = progress.get("chapter_count", 0)
    completed = progress.get("completed", False)
    label     = progress.get("story_title") or start_url[:50]
    print(
        f"\n{'✔' if completed else '⏸'} {'Hoàn thành' if completed else 'Tạm dừng'}: "
        f"{label} — {total} chương",
        flush=True,
    )


# ── Private async helpers ─────────────────────────────────────────────────────

# NOTE: _extract_content_ai đã bị xóa (FIX-C).
# Logic của nó được inline trực tiếp vào scrape_one_chapter để có thể
# chia sẻ biến `ai_classify_cache` — tránh gọi AI 2 lần trên cùng payload.


async def _advance_past_visited(
    url: str,
    all_visited: set[str],
    progress: ProgressDict,
    progress_path: str,
    pool: DomainSessionPool,
    pw_pool: PlaywrightPool,
    profiles: dict[str, SiteProfileDict],
    ai_limiter: AIRateLimiter,
) -> str | None:
    print(f"  [Resume] ⏭ Đã cào rồi, bỏ qua: {url[:60]}", flush=True)
    try:
        _, html = await fetch_page(url, pool, pw_pool)
    except Exception:
        return None

    soup, clean = await asyncio.to_thread(_sync_parse_and_clean, html)

    profile: SiteProfileDict = profiles.get(urlparse(url).netloc.lower(), {})

    next_url = find_next_url(soup, url, profile)
    if not next_url:
        result: AiClassifyResult | None = await ai_classify_and_find(clean, url, ai_limiter)
        if result:
            next_url = result.get("next_url")

    if next_url and next_url not in all_visited:
        progress["current_url"] = next_url
        await save_progress(progress_path, progress)

    return next_url if (next_url and next_url not in all_visited) else None