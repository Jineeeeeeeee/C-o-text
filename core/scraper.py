"""
core/scraper.py — v7: Simplified Full Scrape Mode.

Sau khi Learning Phase hoàn tất và profile được save:
  - run_novel_task() reset progress, bắt đầu scrape từ Ch.1
  - scrape_one_chapter() dùng profile để extract + format
  - Không có calibration, không có observation, không có profile refinement
  - AI chỉ gọi khi: next URL mất (emergency fallback), page type không rõ

Pipeline per-chapter:
  Fetch → html_filter (hidden + remove_selectors) → extract_chapter (formatter)
  → ads_filter → fingerprint check → save .md → find next URL
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import (
    MAX_CHAPTERS, MAX_CONSECUTIVE_ERRORS, MAX_CONSECUTIVE_TIMEOUTS,
    TIMEOUT_BACKOFF_BASE, RE_CHAP_URL, get_delay,
)
from utils.file_io      import load_progress, save_progress, write_markdown
from utils.string_helpers import is_junk_page, make_fingerprint, normalize_title, slugify_filename, truncate
from utils.ads_filter   import AdsFilter
from utils.types        import ProgressDict, SiteProfile
from core.fetch         import fetch_page
from core.html_filter   import prepare_soup
from core.extractor     import extract_chapter
from core.navigator     import find_next_url, detect_page_type
from core.session_pool  import DomainSessionPool, PlaywrightPool
from learning.profile_manager import ProfileManager
from ai.client          import AIRateLimiter
from ai.agents          import ai_classify_and_find, ai_find_first_chapter

logger = logging.getLogger(__name__)


# ── Nav-edge strip ────────────────────────────────────────────────────────────

_RE_WORD_COUNT = re.compile(
    r"^\[\s*[\d,.\s]+words?\s*\]$|^\[\s*\.+\s*words?\s*\]$",
    re.IGNORECASE,
)
_NAV_EDGE_SCAN = 7


def _strip_nav_edges(text: str) -> str:
    """Xóa nav header/footer lặp lại ở đầu/cuối content block."""
    lines = text.splitlines()
    n = len(lines)
    if n < 8:
        return text
    EDGE = _NAV_EDGE_SCAN
    top_set = {lines[i].strip() for i in range(min(EDGE, n)) if lines[i].strip()}
    bot_set = {lines[n-1-i].strip() for i in range(min(EDGE, n)) if lines[n-1-i].strip()}
    repeated = top_set & bot_set

    def _is_nav(line: str) -> bool:
        s = line.strip()
        if not s: return True
        if _RE_WORD_COUNT.match(s): return True
        if len(s) <= 10 and re.match(r"^[A-Za-z\s]+$", s): return True
        return s in repeated

    last_top_nav = -1
    for i in range(min(EDGE, n)):
        if _is_nav(lines[i]):
            last_top_nav = i
    start = last_top_nav + 1
    while start < n and not lines[start].strip():
        start += 1
    end = n
    for i in range(min(EDGE, n)):
        idx = n - 1 - i
        if idx <= start: break
        if not lines[idx].strip() or _is_nav(lines[idx]): end = idx
        else: break
    while end > start and not lines[end-1].strip():
        end -= 1
    return "\n".join(lines[start:end]) if start < end else text


# ── Find start chapter ────────────────────────────────────────────────────────

async def find_start_chapter(
    start_url     : str,
    progress_path : str,
    pool          : DomainSessionPool,
    pw_pool       : PlaywrightPool,
    ai_limiter    : AIRateLimiter,
    profile       : SiteProfile,
) -> tuple[str, ProgressDict]:
    """
    Xác định URL chapter đầu tiên cần scrape.
    Resume từ progress nếu có. Detect index page nếu là lần đầu.
    """
    progress = await load_progress(progress_path)

    # Resume
    if progress.get("current_url"):
        print(f"  [Resume] ▶ {progress['current_url'][:70]}", flush=True)
        return progress["current_url"], progress  # type: ignore[return-value]

    if progress.get("completed"):
        raise RuntimeError("Truyện đã hoàn thành. Xóa progress file để scrape lại.")

    # Fetch start URL
    status, html = await fetch_page(start_url, pool, pw_pool)
    if is_junk_page(html, status):
        raise RuntimeError(f"Trang khởi đầu lỗi (status={status}): {start_url}")

    # Detect page type
    soup      = BeautifulSoup(html, "html.parser")
    page_type = detect_page_type(soup, start_url)

    if page_type == "chapter" and RE_CHAP_URL.search(start_url):
        print(f"  [Start] 📖 Chapter page: {start_url[:70]}", flush=True)
        progress["start_url"] = start_url
        return start_url, progress

    # Index page hoặc không rõ → tìm Ch.1
    print(f"  [Start] 📋 Index/unknown page → tìm Chapter 1...", flush=True)
    first_url = await ai_find_first_chapter(html, start_url, ai_limiter)
    if first_url and first_url != start_url:
        print(f"  [Start] ✅ Chapter 1: {first_url[:70]}", flush=True)
        progress["start_url"] = start_url
        return first_url, progress

    # AI classify fallback
    result = await ai_classify_and_find(html, start_url, ai_limiter)
    if result:
        if result.get("page_type") == "chapter" and RE_CHAP_URL.search(start_url):
            progress["start_url"] = start_url
            return start_url, progress
        for key in ("first_chapter_url", "next_url"):
            found = result.get(key)
            if found and found != start_url:
                print(f"  [Start] ✅ AI: {found[:70]}", flush=True)
                progress["start_url"] = start_url
                return found, progress

    raise RuntimeError(f"Không tìm được điểm bắt đầu: {start_url}")


# ── Scrape one chapter ────────────────────────────────────────────────────────

async def scrape_one_chapter(
    url          : str,
    progress     : ProgressDict,
    progress_path: str,
    output_dir   : str,
    pool         : DomainSessionPool,
    pw_pool      : PlaywrightPool,
    profile      : SiteProfile,
    ai_limiter   : AIRateLimiter,
    ads_filter   : AdsFilter,
) -> str | None:
    """
    Scrape một chapter: fetch → clean → extract → format → save → next URL.

    Returns:
        next_url nếu thành công
        None nếu hết truyện hoặc lỗi
    """
    all_visited: set[str] = set(progress.get("all_visited_urls") or [])
    fingerprints: set[str] = set(progress.get("fingerprints") or [])

    # ── Đã thăm rồi? ──────────────────────────────────────────────────────────
    if url in all_visited:
        print(f"  [Skip] ⏭ Đã thăm: {url[:60]}", flush=True)
        return await _find_next_and_save(url, progress, progress_path, pool, pw_pool, profile, ai_limiter)

    # ── Fetch ─────────────────────────────────────────────────────────────────
    status, html = await fetch_page(url, pool, pw_pool)
    if is_junk_page(html, status):
        print(f"  [End] 🏁 Junk/hết truyện: {url[:60]}", flush=True)
        return None

    # ── Parse + clean ─────────────────────────────────────────────────────────
    soup = await asyncio.to_thread(
        prepare_soup, html, profile.get("remove_selectors")
    )

    # Guard: nếu trang là Index → dừng
    if not RE_CHAP_URL.search(url):
        page_type = detect_page_type(soup, url)
        if page_type == "index":
            print(f"  ⚠️  [Guard] INDEX page — dừng: {url[:70]}", flush=True)
            progress["completed"]        = True
            progress["completed_at_url"] = url
            await save_progress(progress_path, progress)
            return None

    # ── Extract content + title ───────────────────────────────────────────────
    content, title, selector = await asyncio.to_thread(extract_chapter, soup, url, profile)

    if not content or len(content.strip()) < 100:
        # Emergency: thử AI classify để tìm next URL
        print(f"  [Skip] {len((content or '').strip())} chars — bỏ qua: {url[:60]}", flush=True)
        return await _find_next_and_save(url, progress, progress_path, pool, pw_pool, profile, ai_limiter)

    # ── Nav-edge strip ────────────────────────────────────────────────────────
    stripped = _strip_nav_edges(content)
    if stripped and len(stripped.strip()) >= 100:
        content = stripped

    # ── Ads filter ────────────────────────────────────────────────────────────
    before_len = len(content)
    content    = ads_filter.filter(content)
    removed    = before_len - len(content)
    if removed > 50:
        print(f"  [Ads] 🧹 -{removed} chars", flush=True)

    # ── Fingerprint dedup ─────────────────────────────────────────────────────
    fp = make_fingerprint(content)
    if fp in fingerprints:
        print(f"  [Loop] ♻ Lặp nội dung: {url[:60]}", flush=True)
        return None
    fingerprints.add(fp)

    # ── Story title (ch.1 only) ───────────────────────────────────────────────
    if not progress.get("story_title") and progress.get("chapter_count", 0) == 0:
        # Heuristic: tên truyện thường nằm trước "|", "–", "—" trong <title>
        title_tag = soup.find("title")
        if title_tag:
            raw = title_tag.get_text(strip=True)
            m = re.search(r"[\|–—]", raw)
            if m:
                story_candidate = normalize_title(raw[: m.start()].strip())
                if len(story_candidate) > 3:
                    progress["story_title"] = story_candidate

    # ── Save file ─────────────────────────────────────────────────────────────
    chapter_num = progress.get("chapter_count", 0) + 1
    filename    = f"{chapter_num:04d}_{slugify_filename(title, max_len=60)}.md"
    filepath    = os.path.join(output_dir, filename)
    await write_markdown(filepath, f"# {title}\n\n{content}\n")

    # ── Update progress ───────────────────────────────────────────────────────
    progress["chapter_count"]    = chapter_num
    progress["last_title"]       = title
    progress["last_scraped_url"] = url
    all_visited.add(url)
    progress["all_visited_urls"] = list(all_visited)
    progress["fingerprints"]     = list(fingerprints)

    print(
        f"  ✅ Ch.{chapter_num:>4}: "
        f"{truncate(title, 45):<45} | {len(content):>5} chars",
        flush=True,
    )

    # ── Find next URL ─────────────────────────────────────────────────────────
    next_url = find_next_url(soup, url, profile)

    if not next_url:
        # Emergency AI fallback
        try:
            ai_result = await ai_classify_and_find(html, url, ai_limiter)
            if ai_result:
                next_url = ai_result.get("next_url")
        except Exception as e:
            logger.warning("[NextURL] AI fallback thất bại: %s", e)

    if not next_url:
        progress["completed"]        = True
        progress["completed_at_url"] = url
        await save_progress(progress_path, progress)
        print(f"  [End] 🏁 Hết truyện.", flush=True)
        return None

    # Story ID guard
    if not _story_id_ok(next_url, progress):
        print(f"  [Guard] ⛔ URL bị chặn bởi story ID: {next_url[:60]}", flush=True)
        return None

    if next_url in all_visited:
        print(f"  [Loop] ♻ next_url đã thăm: {next_url[:60]}", flush=True)
        return None

    progress["current_url"] = next_url
    await save_progress(progress_path, progress)
    return next_url


async def _find_next_and_save(
    url, progress, progress_path, pool, pw_pool, profile, ai_limiter
) -> str | None:
    """Helper: tìm next URL cho trang đã bỏ qua (đã thăm hoặc content rỗng)."""
    try:
        _, html = await fetch_page(url, pool, pw_pool)
    except Exception:
        return None
    soup = BeautifulSoup(html, "html.parser")
    next_url = find_next_url(soup, url, profile)
    if not next_url:
        try:
            ai_result = await ai_classify_and_find(html, url, ai_limiter)
            if ai_result:
                next_url = ai_result.get("next_url")
        except Exception:
            pass
    if next_url:
        all_visited = set(progress.get("all_visited_urls") or [])
        all_visited.add(url)
        progress["all_visited_urls"] = list(all_visited)
        progress["current_url"]      = next_url
        await save_progress(progress_path, progress)
    return next_url


def _story_id_ok(url: str, progress: ProgressDict) -> bool:
    if not progress.get("story_id_locked"):
        return True
    pattern = progress.get("story_id_regex")
    if not pattern:
        return True
    try:
        return bool(re.search(pattern, url))
    except re.error:
        return True


# ── Main task ─────────────────────────────────────────────────────────────────

async def run_novel_task(
    start_url     : str,
    output_dir    : str,
    progress_path : str,
    pool          : DomainSessionPool,
    pw_pool       : PlaywrightPool,
    pm            : ProfileManager,
    ai_limiter    : AIRateLimiter,
    on_chapter_done = None,
) -> None:
    """
    Entry point cho một truyện.

    Flow:
      1. Nếu profile chưa có / cũ → chạy Learning Phase
      2. Reset progress (scrape lại từ đầu với profile đầy đủ)
      3. Full Scrape Mode loop
    """
    os.makedirs(output_dir, exist_ok=True)
    domain     = urlparse(start_url).netloc.lower()
    ads_filter = AdsFilter.load()

    # ── Phase 1 → 2: Learning (nếu cần) ──────────────────────────────────────
    if not pm.has(domain) or not pm.is_profile_fresh(domain):
        from learning.phase import run_learning_phase
        profile = await run_learning_phase(start_url, pool, pw_pool, pm, ai_limiter)
        if profile is None:
            print(f"  [ERR] Learning Phase thất bại cho {domain}. Bỏ qua.", flush=True)
            return

        # Inject learned ads keywords
        injected = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [Ads] +{injected} keywords từ profile ({ads_filter.stats})", flush=True)

        # Reset progress để scrape lại từ Ch.1 với profile đầy đủ
        print(f"\n🔄 Reset progress → scrape lại từ Chapter 1 với profile hoàn chỉnh...", flush=True)
        await save_progress(progress_path, {
            "current_url"     : start_url,
            "chapter_count"   : 0,
            "story_title"     : None,
            "all_visited_urls": [],
            "fingerprints"    : [],
            "story_id"        : None,
            "story_id_regex"  : None,
            "story_id_locked" : False,
            "completed"       : False,
            "completed_at_url": None,
            "learning_done"   : True,
            "start_url"       : start_url,
        })
    else:
        print(f"  [Profile] 📂 {pm.summary(domain)}", flush=True)
        profile = pm.get(domain)
        injected = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [Ads] +{injected} keywords từ profile ({ads_filter.stats})", flush=True)

    # ── Phase 3: Full Scrape Mode ─────────────────────────────────────────────
    try:
        current_url, progress = await find_start_chapter(
            start_url, progress_path, pool, pw_pool, ai_limiter, profile,
        )
    except Exception as e:
        print(f"  [ERR] Không tìm được điểm bắt đầu: {e}", flush=True)
        return

    story_label = progress.get("story_title") or start_url[:50]
    print(f"\n🚀 {story_label}", flush=True)

    consecutive_errors   = 0
    consecutive_timeouts = 0

    while current_url and progress.get("chapter_count", 0) < MAX_CHAPTERS:
        if progress.get("completed"):
            break

        await asyncio.sleep(get_delay(current_url))

        try:
            prev_count = progress.get("chapter_count", 0)
            next_url   = await scrape_one_chapter(
                url           = current_url,
                progress      = progress,
                progress_path = progress_path,
                output_dir    = output_dir,
                pool          = pool,
                pw_pool       = pw_pool,
                profile       = profile,
                ai_limiter    = ai_limiter,
                ads_filter    = ads_filter,
            )
            consecutive_errors   = 0
            consecutive_timeouts = 0

            if on_chapter_done and progress.get("chapter_count", 0) > prev_count:
                await on_chapter_done()

            current_url = next_url

        except asyncio.CancelledError:
            print(f"  [Cancel] 🛑 Progress đã lưu.", flush=True)
            await save_progress(progress_path, progress)
            raise

        except asyncio.TimeoutError:
            consecutive_timeouts += 1
            wait = TIMEOUT_BACKOFF_BASE * consecutive_timeouts
            print(f"  [Timeout #{consecutive_timeouts}] chờ {wait}s", flush=True)
            if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                break
            await asyncio.sleep(wait)

        except Exception as e:
            consecutive_errors += 1
            print(f"  [ERR #{consecutive_errors}] {type(e).__name__}: {e}", flush=True)
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                break

    # ── Finalize ──────────────────────────────────────────────────────────────
    total     = progress.get("chapter_count", 0)
    completed = progress.get("completed", False)
    label     = progress.get("story_title") or start_url[:50]

    await pm.flush()
    await asyncio.to_thread(ads_filter.save)
    print(
        f"  [Ads] 💾 {ads_filter.stats} saved\n"
        f"\n{'✔' if completed else '⏸'} {label} — {total} chapters",
        flush=True,
    )