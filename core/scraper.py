# core/scraper.py
"""
core/scraper.py — Orchestration: fetch → parse → lưu → điều hướng.

FIXES (v4):
  FIX-INDEX-GUARD (Bug 1 — fanfiction.net): 3 path nguy hiểm đều được bịt:

    Path A — check_and_find_start_chapter():
      detect_page_type() trả "chapter" sai cho story index page (do có nút "Next >").
      Fix: nếu URL không khớp RE_CHAP_URL → override thành "index".

    Path B — check_and_find_start_chapter():
      ai_classify_and_find() trả {"page_type":"chapter"} cho story index page
      → code cũ return start_url làm chapter start.
      Fix: trong "index" path, loại bỏ hoàn toàn branch return start_url từ AI classify.
           Chỉ sử dụng first_chapter_url / next_url khác với start_url.

    Path C — scrape_one_chapter():
      progress["current_url"] bị lưu sai (= story index URL) từ run trước.
      Resume thẳng vào URL đó không qua check_and_find_start_chapter().
      Fix: guard đầu tiên — nếu URL không có số chương VÀ detect_page_type="index"
           → đánh dấu completed, return None, không extract content.

  FIX-NAV-EDGES (Bug 2 — novelfire):
    div.chapter-content capture toàn bộ wrapper gồm breadcrumb + word count +
    chapter nav title ở đầu/cuối.
    Fix: _strip_nav_edges() post-process content sau extract, trước save.
    Thuật toán 3 bước (xem docstring hàm).

  FIX-CONTENT-PRIORITY (v3 — giữ nguyên):
    CONTENT_SELECTORS ưu tiên trước profile content_selector.

  BUG-1 FIX (v2 — giữ nguyên):
    CancelledError lưu progress trước khi re-raise.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from urllib.parse import urlparse
from utils.ads_filter import SimpleAdsFilter
from bs4 import BeautifulSoup

from config import (
    CONTENT_SELECTORS,
    MAX_CHAPTERS,
    MAX_CONSECUTIVE_ERRORS,
    MAX_CONSECUTIVE_TIMEOUTS,
    TIMEOUT_BACKOFF_BASE,
    STORY_ID_LEARN_AFTER,
    STORY_ID_MAX_ATTEMPTS,
    ADS_AI_SCAN_EVERY,
    RE_CHAP_URL,
    get_delay_seconds,
)
from utils.file_io import load_progress, save_progress, write_markdown, save_profiles
from utils.string_helpers import (
    is_junk_page, make_fingerprint, clean_chapter_text,
    normalize_title, slugify_filename, truncate,
    extract_text_blocks,
)
from utils.types import AiClassifyResult, ProgressDict, SiteProfileDict, StoryIdResult
from utils.ads_filter import SimpleAdsFilter
from ai.client  import AIRateLimiter
from ai.agents  import (
    ask_ai_for_story_id,
    ai_find_first_chapter_url,
    ai_classify_and_find,
    ask_ai_build_profile,
    ask_ai_confirm_same_story,
    ai_detect_ads_content,
)
from core.fetch       import fetch_page
from core.navigator   import find_next_url, detect_page_type
from core.html_filter import remove_hidden_elements
from core.extractors  import TitleExtractor, extract_story_title
from core.session_pool import DomainSessionPool, PlaywrightPool

logger = logging.getLogger(__name__)

_COLLECTED_URL_CAP = 20


# ── Nav-edge stripping (FIX-NAV-EDGES) ───────────────────────────────────────

# Khớp: "[ 1234 words ]", "[ 1,234 words ]", "[ ... words ]", "[ 1.2k words ]"
_RE_WORD_COUNT_LINE = re.compile(
    r"^\[\s*[\d,.\s]+words?\s*\]$"
    r"|^\[\s*\.+\s*words?\s*\]$",
    re.IGNORECASE,
)
_NAV_EDGE_SCAN = 7   # số dòng kiểm tra ở mỗi đầu content


def _strip_nav_edges(text: str) -> str:
    """
    Loại bỏ navigation/breadcrumb/footer do aggregator site inject vào đầu/cuối content.

    Pattern điển hình của novelfire.net (và các site tương tự):
      ĐẦU:  "The Primal Hunter"          ← breadcrumb (story title)
            "Chapter 2 - Introduction"   ← chapter nav element
            "[ 1234 words ]"             ← word count
            ""
            "Chapter 2 - Introduction"   ← nav element lặp lại
      CUỐI: "Chapter 2 - Introduction"   ← nav element
            "Report"                     ← button
            "Chapter 2 - Introduction"   ← nav element lặp lại

    Thuật toán 3 bước:
      1. DETECT "repeated lines": các dòng xuất hiện ở CẢ top lẫn bottom EDGE
         dòng → xác nhận là nav element (ví dụ: "Chapter 2 - Introduction").
      2. TOP SCAN (không break): quét _NAV_EDGE_SCAN dòng đầu, ghi nhớ vị trí
         nav cuối cùng (last_top_nav). Mọi thứ <= last_top_nav bị cắt, kể cả
         breadcrumb không phải nav thuần ("The Primal Hunter") nằm trước các
         nav lines vì nó có index < last_top_nav.
      3. BOT SCAN (break tại dòng content thực): quét từ cuối lên, cắt blank/nav,
         DỪNG tại dòng content thực đầu tiên gặp được.

    Safety: nếu sau trim content còn lại rỗng → trả về text gốc.
    """
    lines = text.splitlines()
    n = len(lines)
    if n < 8:
        return text

    EDGE = _NAV_EDGE_SCAN

    # ── Bước 1: phát hiện repeated nav lines ──────────────────────────────────
    top_set = {lines[i].strip() for i in range(min(EDGE, n)) if lines[i].strip()}
    bot_set = {lines[n - 1 - i].strip() for i in range(min(EDGE, n)) if lines[n - 1 - i].strip()}
    repeated = top_set & bot_set   # e.g. {"Chapter 2 - Introduction"}

    def _is_nav(line: str) -> bool:
        s = line.strip()
        if not s:
            return True                                           # blank line
        if _RE_WORD_COUNT_LINE.match(s):
            return True                                           # "[ 1234 words ]"
        if len(s) <= 10 and re.match(r"^[A-Za-z\s]+$", s):
            return True                                           # "Report", "Next", "Prev"
        return s in repeated                                      # chapter title lặp lại ở 2 đầu

    # ── Bước 2: top scan (no-break) — tìm nav line cuối trong EDGE dòng đầu ──
    last_top_nav = -1
    for i in range(min(EDGE, n)):
        if _is_nav(lines[i]):
            last_top_nav = i
        # KHÔNG break: nav lines SAU breadcrumb đẩy last_top_nav vượt qua breadcrumb,
        # khiến breadcrumb cũng bị trim mặc dù bản thân nó không phải nav thuần.

    start = last_top_nav + 1
    # Bỏ qua blank lines ngay sau nav header
    while start < n and not lines[start].strip():
        start += 1

    # ── Bước 3: bot scan (break on real line) — dừng tại content thực ─────────
    end = n
    for i in range(min(EDGE, n)):
        idx = n - 1 - i
        if idx <= start:
            break
        s = lines[idx].strip()
        if not s or _is_nav(lines[idx]):
            end = idx           # blank hoặc nav → tiếp tục trim
        else:
            break               # content thực → DỪNG

    # Bỏ blank lines ngay trước nav footer
    while end > start and not lines[end - 1].strip():
        end -= 1

    if start >= end:
        return text             # safety: không xóa hết

    return "\n".join(lines[start:end])


# ── CPU-bound helpers ─────────────────────────────────────────────────────────

def _sync_parse_and_clean(html: str) -> tuple[BeautifulSoup, str]:
    soup = BeautifulSoup(html, "html.parser")
    remove_hidden_elements(soup)
    return soup, str(soup)


def _sync_detect_page_type(html: str, url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return detect_page_type(soup, url)


def _sync_extract_content(
    soup: BeautifulSoup,
    profile: SiteProfileDict | None = None,
) -> str | None:
    """
    Trích nội dung chương từ soup.

    FIX-CONTENT-PRIORITY (v3):
      1. CONTENT_SELECTORS (hand-crafted, high confidence) — ưu tiên tuyệt đối
      2. profile["content_selector"] (AI-generated) — chỉ là fallback
    """
    # ── Bước 1: CONTENT_SELECTORS ─────────────────────────────────────────────
    for sel in CONTENT_SELECTORS:
        try:
            el = soup.select_one(sel)
            if el:
                text = extract_text_blocks(el)
                if len(text.strip()) > 200:
                    logger.debug(
                        "[Extract] CONTENT_SELECTORS hit: %r → %d chars",
                        sel, len(text.strip()),
                    )
                    return text
        except Exception:
            continue

    # ── Bước 2: Profile selector (fallback only) ──────────────────────────────
    if profile:
        sel = profile.get("content_selector")
        if sel:
            try:
                el = soup.select_one(sel)
                if el:
                    text = extract_text_blocks(el)
                    if len(text.strip()) > 200:
                        logger.debug(
                            "[Extract] Profile fallback selector %r → %d chars",
                            sel, len(text.strip()),
                        )
                        return text
                    logger.debug(
                        "[Extract] Profile selector %r quá ngắn (%d chars), bỏ qua",
                        sel, len(text.strip()),
                    )
            except Exception as e:
                logger.debug("[Extract] Profile selector %r lỗi: %s", sel, e)

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

    # ── FIX-INDEX-GUARD / Path A ──────────────────────────────────────────────
    # detect_page_type() có thể vote "chapter" sai vì trang index có nút "Next >"
    # (bump chapter score +1) mặc dù URL không có số chương.
    # Nếu RE_CHAP_URL không match URL → chắc chắn là index/other.
    # Ví dụ: fanfiction.net /s/14427661 bị phân loại sai thành "chapter".
    if page_type == "chapter" and not RE_CHAP_URL.search(start_url):
        logger.debug(
            "[Start] Path-A override: URL không khớp RE_CHAP_URL (%s)"
            " nhưng detect='chapter' → force 'index'",
            start_url,
        )
        page_type = "index"

    if page_type == "chapter":
        # Verify: có thực sự tồn tại chapter content không?
        soup_check, _ = await asyncio.to_thread(_sync_parse_and_clean, html)
        content_check = await asyncio.to_thread(_sync_extract_content, soup_check, None)

        if content_check and len(content_check.strip()) > 200:
            print(f"  [Start] 📖 Bắt đầu từ chương: {start_url[:70]}", flush=True)
            return start_url, progress

        print(
            f"  [Start] 🔄 Detect 'chapter' nhưng không tìm thấy content"
            f" → fallback sang tìm chương đầu...",
            flush=True,
        )

    # ── Index path ────────────────────────────────────────────────────────────
    print(f"  [Start] 📋 Tìm chương đầu từ trang index...", flush=True)

    # Bước 1: Heuristic — trích chapter links từ HTML
    first_url = await ai_find_first_chapter_url(html, start_url, ai_limiter)
    if first_url and first_url != start_url:
        print(f"  [Start] ✅ Chương đầu: {first_url[:70]}", flush=True)
        return first_url, progress

    # Bước 2: AI full-page classify fallback
    print(f"  [Start] 🤖 Nhờ AI phân tích toàn trang...", flush=True)
    result: AiClassifyResult | None = await ai_classify_and_find(html, start_url, ai_limiter)
    if result:
        if result.get("page_type") == "chapter":
            # ── FIX-INDEX-GUARD / Path B (revised) ───────────────────────────
            # Điều kiện cũ: luôn block return start_url → fanfiction.net bị lỗi
            # vì URL /s/14427661/1/ là chapter thật nhưng content extraction thất bại.
            #
            # Điều kiện mới: cho phép return start_url NẾU cả 2 đều xác nhận chapter:
            #   1. RE_CHAP_URL match URL (URL có pattern số chương rõ ràng)
            #   2. AI classify trả "chapter"
            # → An toàn vì fanfiction /s/{id}/{num}/ và tương tự đều có số chương.
            # scrape_one_chapter() sẽ xử lý content qua AI body fallback nếu cần.
            #
            # Vẫn block nếu URL KHÔNG có pattern số chương (e.g. trang index giả mạo).
            if RE_CHAP_URL.search(start_url):
                print(
                    f"  [Start] ✅ URL pattern + AI xác nhận đây là trang chương: "
                    f"{start_url[:70]}",
                    flush=True,
                )
                return start_url, progress
            else:
                logger.debug(
                    "[Start] Path-B: AI classify='chapter' nhưng URL không có số chương (%s)"
                    " → vẫn bỏ qua, chỉ dùng URL từ AI",
                    start_url,
                )

        for key in ("first_chapter_url", "next_url"):
            found = result.get(key)  # type: ignore[literal-required]
            if found and found != start_url:
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
    ads_filter: SimpleAdsFilter,
) -> str | None:
    all_visited: set[str] = set(progress.get("all_visited_urls") or [])

    if url in all_visited:
        return await _advance_past_visited(url, all_visited, progress, progress_path,
                                           pool, pw_pool, profiles, ai_limiter)

    status, html = await fetch_page(url, pool, pw_pool)
    if is_junk_page(html, status):
        print(f"  [End] 🏁 Hết truyện hoặc trang lỗi: {url[:60]}", flush=True)
        return None

    soup, clean_html = await asyncio.to_thread(_sync_parse_and_clean, html)

    # ── FIX-INDEX-GUARD / Path C ──────────────────────────────────────────────
    # Guard chống story INDEX URL được resume vào scrape_one_chapter().
    # Nguyên nhân: progress["current_url"] bị lưu sai từ run cũ (bug đã fix),
    # nhưng progress file cũ vẫn còn → resume thẳng vào URL sai.
    #
    # Điều kiện kích hoạt (AND cả hai):
    #   1. URL không có số chương (RE_CHAP_URL không match) → URL nghi ngờ
    #   2. detect_page_type() trả "index" → xác nhận là index page
    #
    # Hành động: đánh dấu completed (ngăn vòng lặp vô tận) + return None.
    # User sẽ thấy cảnh báo rõ ràng và biết cần reset progress file.
    if not RE_CHAP_URL.search(url):
        page_type_guard = await asyncio.to_thread(_sync_detect_page_type, html, url)
        if page_type_guard == "index":
            print(
                f"\n  ⚠️  [Guard] URL trỏ đến story INDEX page, không phải chapter!\n"
                f"     URL: {url[:70]}\n"
                f"     Nguyên nhân có thể: progress file cũ lưu sai current_url.\n"
                f"     👉 Hãy xóa progress file tương ứng và chạy lại.\n",
                flush=True,
            )
            progress["completed"]        = True
            progress["completed_at_url"] = url
            await save_progress(progress_path, progress)
            return None

    domain = urlparse(url).netloc.lower()

    # ── Extract content (FIX-CONTENT-PRIORITY v3 + FIX-PROFILE-ORDER v3) ─────
    # Thứ tự: CONTENT_SELECTORS (no profile) → profile/build → AI body fallback

    # Bước 1: CONTENT_SELECTORS không cần profile
    content = await asyncio.to_thread(_sync_extract_content, soup, None)

    ai_classify_cache: AiClassifyResult | None = None

    # Bước 2: CONTENT_SELECTORS thất bại → load/build profile → thử lại
    if content is None:
        profile: SiteProfileDict = profiles.get(domain, {})

        if not profile:
            print(
                f"  [Profile] 🔍 CONTENT_SELECTORS thất bại,"
                f" build profile cho {domain}...",
                flush=True,
            )
            new_profile = await ask_ai_build_profile(clean_html, url, ai_limiter)
            if new_profile:
                await _save_new_profile(profiles, domain, new_profile, profiles_lock)
                profile = new_profile
                print(f"  [Profile] ✅ Đã lưu profile cho {domain}", flush=True)
                if new_profile.get("content_selector"):
                    print(
                        f"  [Profile] content_selector: {new_profile['content_selector']!r}",
                        flush=True,
                    )

        if profile:
            content = await asyncio.to_thread(_sync_extract_content, soup, profile)

    else:
        # CONTENT_SELECTORS thành công → load profile (next_selector, title_selector)
        # Build ngầm nếu domain mới, chỉ ở chương đầu tiên
        profile = profiles.get(domain, {})
        if not profile and progress.get("chapter_count", 0) == 0:
            new_profile = await ask_ai_build_profile(clean_html, url, ai_limiter)
            if new_profile:
                await _save_new_profile(profiles, domain, new_profile, profiles_lock)
                profile = new_profile
                print(
                    f"  [Profile] ✅ Đã lưu profile cho {domain} (background build)",
                    flush=True,
                )

    # Bước 3: AI body fallback — toàn bộ <body>
    if content is None:
        ai_classify_cache = await ai_classify_and_find(clean_html, url, ai_limiter)
        if ai_classify_cache and ai_classify_cache.get("page_type") == "chapter":
            body = soup.find("body")
            if body:
                content = extract_text_blocks(body)

    if not content or len(content.strip()) < 100:
        print(f"  [Skip] Không trích được nội dung: {url[:60]}", flush=True)
        return None

    content = clean_chapter_text(content)

    # ── FIX-NAV-EDGES ─────────────────────────────────────────────────────────
    # Strip nav elements ở đầu/cuối (breadcrumb, chapter title, word count, buttons).
    # Chạy TRƯỚC ads_filter để tránh nav lines bị classify nhầm là ads.
    content_stripped = _strip_nav_edges(content)
    if content_stripped and len(content_stripped.strip()) >= 100:
        if content_stripped != content:
            stripped_lines = content.count("\n") - content_stripped.count("\n")
            logger.debug("[NavEdge] Stripped ~%d lines of nav content", stripped_lines)
        content = content_stripped

    content_before_filter = content
    content = ads_filter.filter_content(content)

    removed_chars = len(content_before_filter) - len(content)
    if removed_chars > 0:                      # ← 4 spaces indent, inside function
        after_set     = set(content.splitlines())
        removed_lines = [
            l.strip() for l in content_before_filter.splitlines()
            if l.strip() and l not in after_set
        ]
        preview = " | ".join(removed_lines[:3])
        print(
            f"  [Ads] 🧹 Đã lọc {removed_chars} ký tự: "
            f"{preview[:100]}{'…' if len(preview) > 100 else ''}",
            flush=True,
        )

    fp           = make_fingerprint(content)
    fingerprints = set(progress.get("fingerprints") or [])
    if fp in fingerprints:
        print(f"  [Loop] ♻ Nội dung lặp lại: {url[:60]}", flush=True)
        return None
    fingerprints.add(fp)
    progress["fingerprints"] = list(fingerprints)

    title = normalize_title(await title_extractor.extract(soup, url, ai_limiter))

    if progress.get("chapter_count", 0) == 0 and not progress.get("story_title"):
        story_title = extract_story_title(soup, url)
        if story_title:
            progress["story_title"] = story_title

    chapter_num = progress.get("chapter_count", 0) + 1

    if chapter_num % ADS_AI_SCAN_EVERY == 1:
        context_block = ads_filter.build_ai_context_block(content_before_filter)
        if context_block:
            print(
                f"  [Ads] 🤖 AI scan watermark (ch.{chapter_num}, "
                f"{ads_filter.keyword_count} kw / {ads_filter.pattern_count} pat)...",
                flush=True,
            )
            raw_result = await ai_detect_ads_content(context_block, ai_limiter)
            if raw_result:
                added = ads_filter.update_from_ai_result(raw_result)
                if added:
                    print(
                        f"  [Ads] ✅ Học thêm {added} pattern mới "
                        f"(tổng: {ads_filter.keyword_count} kw / {ads_filter.pattern_count} pat)",
                        flush=True,
                    )
                else:
                    print(f"  [Ads] ✔ Nội dung sạch, không có pattern mới", flush=True)

    filename     = f"{chapter_num:04d}_{slugify_filename(title, max_len=60)}.md"
    file_content = f"# {title}\n\n{content}\n"
    await write_markdown(os.path.join(output_dir, filename), file_content)

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

    next_url = find_next_url(soup, url, profile)
    if not next_url:
        if ai_classify_cache is None:
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

    current_domain = urlparse(url).netloc
    next_domain    = urlparse(next_url).netloc

    if (
        not progress.get("story_id_locked")
        and next_domain != current_domain
    ):
        print(
            f"  [Guard] ⚠️ Domain thay đổi: {current_domain} → {next_domain}",
            flush=True,
        )
        is_same = await ask_ai_confirm_same_story(
            title1     = title,
            url1       = url,
            title2     = "",
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
    ads_filter           = SimpleAdsFilter()
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
                ads_filter      = ads_filter,
            )
            consecutive_errors   = 0
            consecutive_timeouts = 0

            if on_chapter_done and progress.get("chapter_count", 0) > prev_count:
                await on_chapter_done()

            current_url = next_url

        except asyncio.CancelledError:
            # BUG-1 FIX: lưu progress trước khi re-raise.
            print(
                f"  [Cancel] 🛑 Task bị ngắt, đang lưu progress tại ch."
                f"{progress.get('chapter_count', 0)}...",
                flush=True,
            )
            try:
                await save_progress(progress_path, progress)
            except Exception:
                pass
            raise

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
    await asyncio.to_thread(ads_filter.save)
    print(
        f"  [Ads] 💾 Đã lưu {ads_filter.keyword_count} kw"
        f" / {ads_filter.pattern_count} pat → {_ADS_DB_FILE}",
        flush=True,
    )
    print(
        f"\n{'✔' if completed else '⏸'} {'Hoàn thành' if completed else 'Tạm dừng'}: "
        f"{label} — {total} chương",
        flush=True,
    )


# ── Private async helpers ─────────────────────────────────────────────────────

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

    domain  = urlparse(url).netloc.lower()
    profile: SiteProfileDict = profiles.get(domain, {})

    next_url = find_next_url(soup, url, profile)
    if not next_url:
        result: AiClassifyResult | None = await ai_classify_and_find(clean, url, ai_limiter)
        if result:
            next_url = result.get("next_url")

    if next_url and next_url not in all_visited:
        progress["current_url"] = next_url
        await save_progress(progress_path, progress)

    return next_url if (next_url and next_url not in all_visited) else None