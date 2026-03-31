# core/scraper.py
"""
core/scraper.py — Orchestration: fetch → parse → lưu → điều hướng.

Module này chỉ chứa logic điều phối cao cấp.
Các helper đã được tách ra:
  core/fetch.py        — fetch_page (CF fallback)
  core/navigator.py    — find_next_url, detect_page_type
  core/html_filter.py  — remove_hidden_elements
  core/extractors.py   — TitleExtractor, extract_story_title

Tối ưu CPU: BeautifulSoup parsing + remove_hidden_elements là tác vụ
CPU-bound (synchronous). Tất cả được chạy qua asyncio.to_thread() để
tránh block Event Loop khi cào nhiều truyện song song.
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

# ── Collected URLs cap (chỉ cần để học story_id, không cần lưu mãi) ───────────
_COLLECTED_URL_CAP = 20


# ── CPU-bound helpers (chạy trong thread pool) ────────────────────────────────

def _sync_parse_and_clean(html: str) -> tuple[BeautifulSoup, str]:
    """
    Parse HTML và xóa hidden elements — CPU-bound, chạy qua asyncio.to_thread().

    Trả về tuple (soup, clean_html_string) để tránh serialize/deserialize
    soup hai lần: caller dùng soup cho extract, dùng clean_html cho navigator/AI.
    """
    soup = BeautifulSoup(html, "html.parser")
    remove_hidden_elements(soup)
    return soup, str(soup)


def _sync_detect_page_type(html: str, url: str) -> str:
    """Wrapper sync cho detect_page_type — gọi qua asyncio.to_thread()."""
    return detect_page_type(html, url)


def _sync_extract_content(soup: BeautifulSoup) -> str | None:
    """
    Thử các CSS selector có sẵn để lấy nội dung chương — CPU-bound.

    Sync, không gọi AI. Chạy qua asyncio.to_thread() từ scrape_one_chapter.
    """
    for sel in CONTENT_SELECTORS:
        el = soup.select_one(sel)
        if el:
            text = el.get_text("\n", strip=False)
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
    """Trả về False nếu URL không khớp regex của truyện hiện tại."""
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

    # detect_page_type cũng parse HTML → đẩy xuống thread
    page_type = await asyncio.to_thread(_sync_detect_page_type, html, start_url)

    if page_type == "chapter":
        print(f"  [Start] 📖 Bắt đầu từ chương: {start_url[:70]}", flush=True)
        return start_url, progress

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
    """
    Cào một chương: fetch → clean → extract → lưu .md → tìm URL tiếp.

    Trả về URL chương tiếp theo, hoặc None nếu hết truyện / lỗi.
    """
    all_visited: set[str] = set(progress.get("all_visited_urls") or [])

    # ── Resume: URL đã cào, chỉ lấy next_url ─────────────────────────
    if url in all_visited:
        return await _advance_past_visited(url, all_visited, progress, progress_path,
                                           pool, pw_pool, profiles, ai_limiter)

    # ── Fetch ─────────────────────────────────────────────────────────
    status, html = await fetch_page(url, pool, pw_pool)
    if is_junk_page(html, status):
        print(f"  [End] 🏁 Hết truyện hoặc trang lỗi: {url[:60]}", flush=True)
        return None

    # ── CPU-bound: parse + clean trong thread pool ────────────────────
    # Tránh block Event Loop khi HTML lớn (1–4MB không phải hiếm).
    # Trả về cả soup (cho extract) và clean_html string (cho navigator/AI).
    soup, clean_html = await asyncio.to_thread(_sync_parse_and_clean, html)

    domain  = urlparse(url).netloc.lower()

    # ── Profile: tạo mới nếu domain chưa biết ────────────────────────
    profile: SiteProfileDict = profiles.get(domain, {})  # type: ignore[assignment]
    if not profile:
        new_profile = await ask_ai_build_profile(clean_html, url, ai_limiter)
        if new_profile:
            await _save_new_profile(profiles, domain, new_profile, profiles_lock)
            profile = new_profile
            print(f"  [Profile] ✅ Đã lưu profile cho {domain}", flush=True)

    # ── Extract nội dung (sync, CPU-bound → thread pool) ─────────────
    content = await asyncio.to_thread(_sync_extract_content, soup)
    if content is None:
        content = await _extract_content_ai(soup, clean_html, url, ai_limiter)

    if not content or len(content.strip()) < 100:
        print(f"  [Skip] Không trích được nội dung: {url[:60]}", flush=True)
        return None

    content = clean_chapter_text(content)

    # ── Phát hiện vòng lặp nội dung ──────────────────────────────────
    fp           = make_fingerprint(content)
    fingerprints = set(progress.get("fingerprints") or [])
    if fp in fingerprints:
        print(f"  [Loop] ♻ Nội dung lặp lại: {url[:60]}", flush=True)
        return None
    fingerprints.add(fp)
    progress["fingerprints"] = list(fingerprints)

    # ── Tiêu đề ───────────────────────────────────────────────────────
    title = normalize_title(await title_extractor.extract(html, url))

    # ── Lưu tên truyện (chỉ chương đầu tiên) ─────────────────────────
    if progress.get("chapter_count", 0) == 0 and not progress.get("story_title"):
        story_title = extract_story_title(soup, url)
        if story_title:
            progress["story_title"] = story_title

    # ── Ghi file .md ──────────────────────────────────────────────────
    chapter_num  = progress.get("chapter_count", 0) + 1
    filename     = f"{chapter_num:04d}_{slugify_filename(title, max_len=60)}.md"
    file_content = f"# {title}\n\n{content}\n\n<!-- {url} -->\n"
    await write_markdown(os.path.join(output_dir, filename), file_content)

    # ── Cập nhật progress ─────────────────────────────────────────────
    progress["chapter_count"]    = chapter_num
    progress["last_title"]       = title
    progress["last_scraped_url"] = url

    all_visited.add(url)
    progress["all_visited_urls"] = list(all_visited)

    # collected_urls: chỉ lưu khi chưa lock story_id, giới hạn kích thước
    if not progress.get("story_id_locked"):
        collected: list[str] = progress.get("collected_urls") or []
        if url not in collected:
            collected.append(url)
        progress["collected_urls"] = collected[-_COLLECTED_URL_CAP:]

        # Học story_id sau đủ mẫu
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

    # ── Tìm URL tiếp theo ─────────────────────────────────────────────
    next_url = find_next_url(clean_html, url, profile)
    if not next_url:
        ai_result: AiClassifyResult | None = await ai_classify_and_find(
            clean_html, url, ai_limiter
        )
        if ai_result:
            next_url = ai_result.get("next_url")

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

    # ── FIX Bug #4: Xác nhận next_url thuộc cùng truyện ─────────────
    last_title = progress.get("last_title", "")
    if last_title and next_url:
        is_same = await ask_ai_confirm_same_story(
            title1     = last_title,
            url1       = url,
            title2     = "",       # chưa fetch → AI dùng URL để phán đoán
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

async def _extract_content_ai(
    soup: BeautifulSoup,
    clean_html: str,
    url: str,
    ai_limiter: AIRateLimiter,
) -> str | None:
    """Fallback: nhờ AI xác nhận đây là trang chương, rồi lấy body text."""
    result: AiClassifyResult | None = await ai_classify_and_find(clean_html, url, ai_limiter)
    if result and result.get("page_type") == "chapter":
        body = soup.find("body")
        if body:
            return body.get_text("\n", strip=False)
    return None


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
    """
    URL đã cào rồi (resume sau Ctrl+C).
    Re-fetch để tìm next_url mà không lưu lại nội dung.
    """
    print(f"  [Resume] ⏭ Đã cào rồi, bỏ qua: {url[:60]}", flush=True)
    try:
        _, html = await fetch_page(url, pool, pw_pool)
    except Exception:
        return None

    # CPU-bound trong thread pool — nhất quán với scrape_one_chapter
    soup, clean = await asyncio.to_thread(_sync_parse_and_clean, html)

    profile: SiteProfileDict = profiles.get(urlparse(url).netloc.lower(), {})  # type: ignore[assignment]

    next_url = find_next_url(clean, url, profile)
    if not next_url:
        result: AiClassifyResult | None = await ai_classify_and_find(clean, url, ai_limiter)
        if result:
            next_url = result.get("next_url")

    if next_url and next_url not in all_visited:
        progress["current_url"] = next_url
        await save_progress(progress_path, progress)

    return next_url if (next_url and next_url not in all_visited) else None