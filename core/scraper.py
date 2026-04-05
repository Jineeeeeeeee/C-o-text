"""
core/scraper.py — v17: 10-call learning phase + title_selector protection.

Thay đổi so với v16:
  LEARN-1: run_learning_phase() giờ trả về 3-tuple:
           (profile, sample_titles, fetched_chapters)
           fetched_chapters được tái dùng → không fetch lại 10 chapters đầu.
  TITLE-1: prepare_soup() nhận thêm title_selector để bảo vệ h1/title
           khỏi bị xóa nhầm bởi remove_selectors (fix RoyalRoad bug).
  TITLE-2: profile["title_selector"] thay vì profile["chapter_title_selector"]
           trong extractor — normalize field name.
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
from utils.file_io        import load_progress, save_progress, write_markdown
from utils.string_helpers import (
    is_junk_page, make_fingerprint, normalize_title, slugify_filename, truncate,
)
from utils.ads_filter     import AdsFilter
from utils.types          import ProgressDict, SiteProfile
from utils.issue_reporter import IssueReporter
from core.fetch           import fetch_page
from core.html_filter     import prepare_soup
from core.extractor       import extract_chapter
from core.navigator       import find_next_url, detect_page_type
from core.session_pool    import DomainSessionPool, PlaywrightPool
from learning.profile_manager import ProfileManager
from ai.client            import AIRateLimiter
from ai.agents            import ai_classify_and_find, ai_find_first_chapter

logger = logging.getLogger(__name__)

_ADS_AUTO_THRESHOLD = 10
_ADS_AI_MIN_COUNT   = 3
_ADS_FREQ_MIN_FILES = 5
MAX_EMPTY_STREAK    = 5


def _dtag(url_or_domain: str) -> str:
    if url_or_domain.startswith("http"):
        netloc = urlparse(url_or_domain).netloc.lower()
    else:
        netloc = url_or_domain.lower()
    name = netloc.replace("www.", "").split(".")[0]
    return f"{name[:12]:<12}"


_CHAPTER_TITLE_RE = re.compile(
    r"^\s*(?:chapter|chap|ch|episode|ep|part|chuong|phan)\b[\s.\-:]*\d*",
    re.IGNORECASE | re.UNICODE,
)
_KNOWN_SITES = frozenset({
    "royalroad", "royal road", "scribblehub", "wattpad", "fanfiction",
    "fanfiction.net", "archiveofourown", "ao3", "webnovel", "novelfire",
    "novelupdates", "lightnovelreader", "novelfull", "wuxiaworld",
})


def _extract_story_title(raw_page_title: str) -> str | None:
    parts = re.split(r"\s*[\|–—]\s*", raw_page_title)
    candidates = []
    for part in parts:
        part = part.strip()
        if len(part) < 3:
            continue
        if _CHAPTER_TITLE_RE.match(part):
            continue
        if part.lower() in _KNOWN_SITES:
            continue
        candidates.append(part)
    if not candidates:
        return None
    return max(candidates, key=len)


_RE_PIPE_SUFFIX = re.compile(r"\s*\|.*$")


def _format_chapter_filename(
    chapter_num: int,
    raw_title  : str,
    progress   : ProgressDict,
) -> str:
    chapter_kw  = (progress.get("chapter_keyword") or "Chapter").strip()
    has_subtitle = bool(progress.get("has_chapter_subtitle", False))
    prefix_strip = (progress.get("story_prefix_strip") or "").strip()

    title = raw_title.strip()
    if prefix_strip:
        lo_title  = title.lower()
        lo_prefix = prefix_strip.lower()
        if lo_title.startswith(lo_prefix):
            title = title[len(prefix_strip):].lstrip(" ,;:-–—")

    kw_esc  = re.escape(chapter_kw)
    chap_re = re.compile(
        rf"(?:{kw_esc})\s*(?P<n>\d+)\s*[-–—:.]?\s*(?P<sub>.*)",
        re.IGNORECASE,
    )
    m = chap_re.search(title)

    if m:
        n       = m.group("n")
        sub_raw = m.group("sub").strip(" -–—:[]().")
        sub_raw = _RE_PIPE_SUFFIX.sub("", sub_raw).strip()
        chap_id = f"{chapter_kw}{n}"
        if has_subtitle and sub_raw and len(sub_raw) >= 2:
            sub_safe = slugify_filename(sub_raw, max_len=50)
            name = f"{chapter_num:04d}_{chap_id}_{sub_safe}"
        else:
            name = f"{chapter_num:04d}_{chap_id}"
    else:
        fallback = _RE_PIPE_SUFFIX.sub("", (title or raw_title)).strip()
        name = f"{chapter_num:04d}_{slugify_filename(fallback, max_len=60)}"

    return slugify_filename(name, max_len=120) + ".md"


_RE_WORD_COUNT = re.compile(
    r"^\[\s*[\d,.\s]+words?\s*\]$|^\[\s*\.+\s*words?\s*\]$",
    re.IGNORECASE,
)
_NAV_EDGE_SCAN = 7


def _strip_nav_edges(text: str) -> str:
    lines = text.splitlines()
    n = len(lines)
    if n < 8:
        return text

    EDGE = _NAV_EDGE_SCAN
    top_set = {lines[i].strip() for i in range(min(EDGE, n)) if lines[i].strip()}
    bot_set = {lines[n - 1 - i].strip() for i in range(min(EDGE, n)) if lines[n - 1 - i].strip()}
    repeated = top_set & bot_set

    def _is_nav(line: str) -> bool:
        s = line.strip()
        if not s:
            return True
        if _RE_WORD_COUNT.match(s):
            return True
        if len(s) <= 10 and re.match(r"^[A-Za-z\s]+$", s):
            return True
        return s in repeated

    start = 0
    for i in range(min(EDGE, n)):
        if _is_nav(lines[i]):
            start = i + 1
        else:
            break

    while start < n and not lines[start].strip():
        start += 1

    end = n
    for i in range(min(EDGE, n)):
        idx = n - 1 - i
        if idx <= start:
            break
        if not lines[idx].strip() or _is_nav(lines[idx]):
            end = idx
        else:
            break

    while end > start and not lines[end - 1].strip():
        end -= 1

    return "\n".join(lines[start:end]) if start < end else text


def _build_story_id_regex(url: str) -> str | None:
    try:
        path     = urlparse(url).path
        segments = [s for s in path.split("/") if s]
        if len(segments) >= 3 and segments[0] == "s" and segments[1].isdigit():
            return re.escape(f"/s/{segments[1]}/")
        for i, seg in enumerate(segments):
            if seg.isdigit() and i > 0:
                story_path = "/" + "/".join(segments[: i + 1]) + "/"
                return re.escape(story_path)
    except Exception:
        pass
    return None


def _is_chapter_url(url: str, profile: SiteProfile) -> bool:
    pattern = profile.get("chapter_url_pattern")
    if pattern:
        try:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        except re.error:
            pass
    return bool(RE_CHAP_URL.search(url))


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


async def find_start_chapter(
    start_url     : str,
    progress_path : str,
    pool          : DomainSessionPool,
    pw_pool       : PlaywrightPool,
    ai_limiter    : AIRateLimiter,
    profile       : SiteProfile,
) -> tuple[str, ProgressDict]:
    progress = await load_progress(progress_path)

    if progress.get("current_url"):
        tag = _dtag(start_url)
        print(f"  [{tag}] ▶  Resume → {progress['current_url'][:65]}", flush=True)
        return progress["current_url"], progress  # type: ignore[return-value]

    if progress.get("completed"):
        raise RuntimeError("Truyện đã hoàn thành. Xóa progress file để scrape lại.")

    status, html = await fetch_page(start_url, pool, pw_pool)
    if is_junk_page(html, status):
        raise RuntimeError(f"Trang khởi đầu lỗi (status={status}): {start_url}")

    soup      = BeautifulSoup(html, "html.parser")
    page_type = detect_page_type(soup, start_url)

    if page_type == "chapter" and _is_chapter_url(start_url, profile):
        tag = _dtag(start_url)
        print(f"  [{tag}] 📖 Start chapter: {start_url[:65]}", flush=True)
        progress["start_url"] = start_url
        return start_url, progress

    tag = _dtag(start_url)
    print(f"  [{tag}] 📋 Index page → tìm Chapter 1...", flush=True)
    first_url = await ai_find_first_chapter(html, start_url, ai_limiter)
    if first_url and first_url != start_url:
        print(f"  [{tag}] ✅ Chapter 1: {first_url[:65]}", flush=True)
        progress["start_url"] = start_url
        return first_url, progress

    result = await ai_classify_and_find(html, start_url, ai_limiter)
    if result:
        if result.get("page_type") == "chapter" and _is_chapter_url(start_url, profile):
            progress["start_url"] = start_url
            return start_url, progress
        for key in ("first_chapter_url", "next_url"):
            found = result.get(key)
            if found and found != start_url:
                print(f"  [{tag}] ✅ AI → {found[:65]}", flush=True)
                progress["start_url"] = start_url
                return found, progress

    raise RuntimeError(f"Không tìm được điểm bắt đầu: {start_url}")


async def scrape_one_chapter(
    url           : str,
    progress      : ProgressDict,
    progress_path : str,
    output_dir    : str,
    pool          : DomainSessionPool,
    pw_pool       : PlaywrightPool,
    profile       : SiteProfile,
    ai_limiter    : AIRateLimiter,
    ads_filter    : AdsFilter,
    issue_reporter: IssueReporter,
    # LEARN-1: Pre-fetched HTML từ learning phase (tránh fetch lại)
    prefetched_html: str | None = None,
) -> str | None:
    tag          = _dtag(url)
    all_visited  : set[str] = set(progress.get("all_visited_urls") or [])
    fingerprints : set[str] = set(progress.get("fingerprints") or [])

    if url in all_visited:
        return await _find_next_and_save(
            url, progress, progress_path, pool, pw_pool, profile, ai_limiter,
            issue_reporter=issue_reporter,
        )

    # LEARN-1: Dùng pre-fetched HTML nếu có, không fetch lại
    if prefetched_html is not None:
        html   = prefetched_html
        status = 200
    else:
        try:
            status, html = await fetch_page(url, pool, pw_pool)
        except Exception as e:
            err_msg = str(e)
            ch_num  = progress.get("chapter_count", 0) + 1
            if any(kw in err_msg.lower() for kw in ("403", "captcha", "cloudflare", "blocked")):
                issue_reporter.report("BLOCKED", url, detail=err_msg[:120], chapter_num=ch_num)
            raise

    if is_junk_page(html, status):
        if status in (403, 429):
            ch_num = progress.get("chapter_count", 0) + 1
            issue_reporter.report("BLOCKED", url, detail=f"HTTP {status}", chapter_num=ch_num)
        print(f"  [{tag}] 🏁 Hết truyện / junk page", flush=True)
        return None

    # TITLE-1: Pass title_selector vào prepare_soup để bảo vệ title element
    soup = await asyncio.to_thread(
        prepare_soup, html,
        profile.get("remove_selectors"),
        profile.get("content_selector"),
        profile.get("title_selector"),   # ← MỚI: title protection
    )

    if not RE_CHAP_URL.search(url):
        page_type = detect_page_type(soup, url)
        if page_type == "index":
            print(f"  [{tag}] ⛔ INDEX page guard — dừng", flush=True)
            progress["completed"]        = True
            progress["completed_at_url"] = url
            await save_progress(progress_path, progress)
            return None

    content, title, selector = await asyncio.to_thread(extract_chapter, soup, url, profile)

    if not content or len(content.strip()) < 100:
        ch_hint = progress.get("chapter_count", 0) + 1
        if selector is None:
            issue_reporter.report(
                "CONTENT_SUSPICIOUS", url,
                detail="content_selector returned 0 chars",
                chapter_num=ch_hint,
            )
        print(f"  [{tag}] ⏭  #{ch_hint:>4}: 0 chars — {truncate(url, 52)}", flush=True)
        return await _find_next_and_save(
            url, progress, progress_path, pool, pw_pool, profile, ai_limiter,
            soup=soup, html=html, issue_reporter=issue_reporter,
        )

    stripped = _strip_nav_edges(content)
    if stripped and len(stripped.strip()) >= 100:
        content = stripped

    if title and re.fullmatch(r"Chapter \d+", title):
        issue_reporter.report(
            "TITLE_FALLBACK", url,
            detail=f"Title extracted as '{title}' — may be URL slug fallback",
            chapter_num=progress.get("chapter_count", 0) + 1,
        )

    content = ads_filter.filter(content, chapter_url=url)

    fp = make_fingerprint(content)
    if fp in fingerprints:
        print(f"  [{tag}] ♻  Loop nội dung — dừng", flush=True)
        return None
    fingerprints.add(fp)

    if not progress.get("story_title") and not progress.get("story_name_clean"):
        if progress.get("chapter_count", 0) == 0:
            title_tag = soup.find("title")
            if title_tag:
                raw = title_tag.get_text(strip=True)
                story_candidate = _extract_story_title(raw)
                if story_candidate:
                    progress["story_title"] = normalize_title(story_candidate)

    chapter_num = progress.get("chapter_count", 0) + 1
    filename    = _format_chapter_filename(chapter_num, title, progress)
    filepath    = os.path.join(output_dir, filename)
    await write_markdown(filepath, f"# {title}\n\n{content}\n")

    ads_filter.scan_edges_for_suspects(
        content,
        chapter_url  = url,
        chapter_file = filepath,
    )

    progress["chapter_count"]    = chapter_num
    progress["last_title"]       = title
    progress["last_scraped_url"] = url
    all_visited.add(url)
    progress["all_visited_urls"] = list(all_visited)
    progress["fingerprints"]     = list(fingerprints)

    print(
        f"  [{tag}] ✅ {chapter_num:>4}: "
        f"{truncate(title, 44):<44}  {len(content):>7,}c",
        flush=True,
    )

    issue_reporter.mark_chapter_ok()

    next_url = find_next_url(soup, url, profile)
    if not next_url:
        try:
            ai_result = await ai_classify_and_find(html, url, ai_limiter)
            if ai_result:
                next_url = ai_result.get("next_url")
        except Exception as e:
            logger.warning("[NextURL] AI fallback thất bại: %s", e)

    if not next_url:
        issue_reporter.report(
            "NEXT_URL_MISSING", url,
            detail="Heuristic + AI fallback both failed",
            chapter_num=chapter_num,
        )
        progress["completed"]        = True
        progress["completed_at_url"] = url
        await save_progress(progress_path, progress)
        print(f"  [{tag}] 🏁 Hết truyện", flush=True)
        return None

    if not _story_id_ok(next_url, progress):
        print(f"  [{tag}] ⛔ Story ID guard: {next_url[:55]}", flush=True)
        return None

    if next_url in all_visited:
        print(f"  [{tag}] ♻  next_url đã thăm — dừng", flush=True)
        return None

    progress["current_url"] = next_url
    await save_progress(progress_path, progress)
    return next_url


async def _find_next_and_save(
    url, progress, progress_path, pool, pw_pool, profile, ai_limiter,
    *, soup=None, html=None, issue_reporter=None,
) -> str | None:
    if soup is None or html is None:
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

    if not next_url and issue_reporter:
        issue_reporter.report("NEXT_URL_MISSING", url, detail="Empty-content chapter")

    if next_url:
        all_visited = set(progress.get("all_visited_urls") or [])
        all_visited.add(url)
        progress["all_visited_urls"] = list(all_visited)
        progress["current_url"]      = next_url
        await save_progress(progress_path, progress)
    return next_url


async def _finalize_ads(
    ads_filter: AdsFilter,
    domain    : str,
    ai_limiter: AIRateLimiter,
    pm        : ProfileManager,
    output_dir: str,
    cancelled : bool,
) -> None:
    """
    FIX-B1: auto_candidates giờ CHỈ chứa JS/script injection lines.
    Tất cả candidates khác đều qua AI verify, kể cả count cao.
    """
    from ai.agents import ai_verify_ads

    domain_slug      = domain.replace(".", "_")
    verified_results : dict[str, bool] = {}

    auto_candidates, ai_candidates = ads_filter.get_candidates_by_frequency(
        auto_threshold = _ADS_AUTO_THRESHOLD,   # vẫn pass nhưng semantics đã thay đổi
        min_count      = _ADS_AI_MIN_COUNT,
        max_results    = 20,
    )

    # Auto-approve CHỈ cho JS injection lines (không cần AI confirm)
    if auto_candidates:
        added = ads_filter.apply_verified(auto_candidates)
        for line in auto_candidates:
            verified_results[line] = True
        if added > 0:
            print(
                f"  [Ads] 🔒 +{added} JS injection auto-learned | {ads_filter.stats}",
                flush=True,
            )
            await pm.add_ads_to_profile(domain, auto_candidates)

    new_suspect_lines = ads_filter.get_new_frequency_suspects(
        min_files=_ADS_FREQ_MIN_FILES, max_results=20,
    )
    all_for_ai      = list(dict.fromkeys(ai_candidates + new_suspect_lines))
    new_suspect_set = set(new_suspect_lines)

    if not cancelled and all_for_ai:
        print(f"  [Ads] 🤖 AI xác nhận {len(all_for_ai)} dòng...", flush=True)
        try:
            confirmed     = await ai_verify_ads(all_for_ai, domain, ai_limiter)
            confirmed_set = set(confirmed)
            for line in all_for_ai:
                verified_results[line] = line in confirmed_set
            if confirmed:
                ads_filter.apply_verified(confirmed)
                confirmed_new = [l for l in confirmed if l in new_suspect_set]
                if confirmed_new:
                    removed_count = await asyncio.to_thread(
                        AdsFilter.post_process_directory, confirmed_new, output_dir,
                    )
                    if removed_count > 0:
                        print(f"  [Ads] ✅ Đã xóa {removed_count} dòng từ files", flush=True)
                await pm.add_ads_to_profile(domain, confirmed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[Ads] AI verify thất bại: %s", e)
    elif cancelled and all_for_ai:
        print(
            f"  [Ads] ⚠ Cancelled — {len(all_for_ai)} candidates chưa verify, "
            f"đã lưu vào pending review",
            flush=True,
        )

    ads_filter.save_pending_review(domain_slug, verified_results or None)
    await asyncio.to_thread(ads_filter.save)
    print(f"  [Ads] 💾 {ads_filter.stats}", flush=True)


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
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    actual_output_dir  = output_dir
    ads_filter         = AdsFilter.load(domain=domain)
    issue_reporter     = IssueReporter(domain=domain)
    pre_fetched_titles : list[str] = []

    # LEARN-1: fetched_chapters từ learning phase — tái dùng, không fetch lại
    fetched_chapters: list[tuple[str, str]] = []

    # ── Phase 1→2: Learning ───────────────────────────────────────────────────
    if not pm.has(domain) or not pm.is_profile_fresh(domain):
        if os.path.exists(progress_path):
            try:
                os.remove(progress_path)
                print(f"  [{tag}] 🗑  Cleared old progress", flush=True)
            except Exception as e:
                logger.warning("[Learn] Failed to clear progress: %s", e)

        from learning.phase import run_learning_phase
        result = await run_learning_phase(start_url, pool, pw_pool, pm, ai_limiter)
        if result is None:
            print(f"  [{tag}] ❌ Learning Phase thất bại. Bỏ qua.", flush=True)
            issue_reporter.report(
                "LEARNING_FAILED", start_url,
                detail="run_learning_phase() returned None",
            )
            issue_reporter.summarize(0)
            return

        # LEARN-1: Unpack 3-tuple (profile, titles, chapters)
        profile, pre_fetched_titles, fetched_chapters = result

        injected = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [{tag}] [Ads] +{injected} từ profile | {ads_filter.stats}", flush=True)

        print(f"\n  [{tag}] 🔄 Tái dùng {len(fetched_chapters)} chapters đã fetch...\n", flush=True)
        await save_progress(progress_path, {
            "current_url"         : None,
            "chapter_count"       : 0,
            "story_title"         : None,
            "all_visited_urls"    : [],
            "fingerprints"        : [],
            "story_id"            : None,
            "story_id_regex"      : None,
            "story_id_locked"     : False,
            "completed"           : False,
            "completed_at_url"    : None,
            "learning_done"       : True,
            "start_url"           : start_url,
            "naming_done"         : False,
            "story_name_clean"    : None,
            "chapter_keyword"     : None,
            "has_chapter_subtitle": False,
            "story_prefix_strip"  : None,
            "output_dir_final"    : None,
        })
    else:
        print(f"  [{tag}] 📂 {pm.summary(domain)}", flush=True)
        profile            = pm.get(domain)
        pre_fetched_titles = []
        fetched_chapters   = []
        injected = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [{tag}] [Ads] +{injected} từ profile | {ads_filter.stats}", flush=True)

    # ── Phase 3: Full Scrape Mode ─────────────────────────────────────────────
    try:
        current_url, progress = await find_start_chapter(
            start_url, progress_path, pool, pw_pool, ai_limiter, profile,
        )
    except Exception as e:
        print(f"  [{tag}] ❌ Không tìm được điểm bắt đầu: {e}", flush=True)
        return

    # ── Naming Phase ──────────────────────────────────────────────────────────
    if not progress.get("naming_done"):
        from learning.naming import run_naming_phase
        naming = await run_naming_phase(
            chapter1_url       = current_url,
            pool               = pool,
            pw_pool            = pw_pool,
            ai_limiter         = ai_limiter,
            profile            = profile,
            pre_fetched_titles = pre_fetched_titles or None,
        )
        if naming:
            for k, v in naming.items():
                progress[k] = v  # type: ignore[literal-required]
        progress["naming_done"] = True
        await save_progress(progress_path, progress)

    if not progress.get("story_id_locked"):
        sid_pattern = _build_story_id_regex(current_url)
        if sid_pattern:
            progress["story_id_regex"]  = sid_pattern
            progress["story_id_locked"] = True
            await save_progress(progress_path, progress)

    actual_output_dir = progress.get("output_dir_final") or output_dir
    os.makedirs(actual_output_dir, exist_ok=True)

    story_label = (
        progress.get("story_name_clean")
        or progress.get("story_title")
        or urlparse(start_url).netloc
    )
    issue_reporter.set_story_label(story_label)

    print(f"\n{'─'*62}", flush=True)
    print(f"  🚀 [{tag}] {story_label}", flush=True)
    print(f"{'─'*62}", flush=True)

    # LEARN-1: Build map url→html từ fetched_chapters để tái dùng
    prefetch_map: dict[str, str] = {url: html for url, html in fetched_chapters}

    consecutive_errors   = 0
    consecutive_timeouts = 0
    consecutive_empty    = 0
    _cancelled           = False

    try:
        while current_url and progress.get("chapter_count", 0) < MAX_CHAPTERS:
            if progress.get("completed"):
                break

            await asyncio.sleep(get_delay(current_url))

            try:
                prev_count = progress.get("chapter_count", 0)

                # LEARN-1: Dùng pre-fetched HTML nếu có cho chapter này
                prefetched = prefetch_map.pop(current_url, None)

                next_url = await scrape_one_chapter(
                    url             = current_url,
                    progress        = progress,
                    progress_path   = progress_path,
                    output_dir      = actual_output_dir,
                    pool            = pool,
                    pw_pool         = pw_pool,
                    profile         = profile,
                    ai_limiter      = ai_limiter,
                    ads_filter      = ads_filter,
                    issue_reporter  = issue_reporter,
                    prefetched_html = prefetched,
                )
                consecutive_errors   = 0
                consecutive_timeouts = 0

                new_count = progress.get("chapter_count", 0)
                if new_count > prev_count:
                    consecutive_empty = 0
                    if on_chapter_done:
                        await on_chapter_done()
                else:
                    consecutive_empty += 1
                    if consecutive_empty >= MAX_EMPTY_STREAK:
                        print(
                            f"\n  [{tag}] ⏸  Tạm dừng: {MAX_EMPTY_STREAK} chương "
                            f"liên tiếp không có nội dung.",
                            flush=True,
                        )
                        issue_reporter.report(
                            "EMPTY_STREAK", current_url,
                            detail=f"{MAX_EMPTY_STREAK} consecutive empty chapters.",
                            chapter_num=progress.get("chapter_count", 0) + 1,
                        )
                        await save_progress(progress_path, progress)
                        break

                current_url = next_url

            except asyncio.CancelledError:
                _cancelled = True
                await save_progress(progress_path, progress)
                print(f"  [{tag}] 🛑 Cancelled — progress saved", flush=True)
                raise

            except asyncio.TimeoutError:
                consecutive_timeouts += 1
                wait = TIMEOUT_BACKOFF_BASE * consecutive_timeouts
                print(f"  [{tag}] ⏱  Timeout #{consecutive_timeouts} — wait {wait}s", flush=True)
                if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                    break
                await asyncio.sleep(wait)

            except Exception as e:
                consecutive_errors += 1
                import traceback
                print(
                    f"  [{tag}] ⚠  ERR #{consecutive_errors}: {type(e).__name__}: {e}\n"
                    f"{traceback.format_exc()}",
                    flush=True,
                )
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    break

    finally:
        total     = progress.get("chapter_count", 0)
        completed = progress.get("completed", False)
        label     = progress.get("story_name_clean") or progress.get("story_title") or start_url[:50]

        issue_reporter.summarize(total)

        try:
            await asyncio.shield(
                _finalize_ads(
                    ads_filter = ads_filter,
                    domain     = domain,
                    ai_limiter = ai_limiter,
                    pm         = pm,
                    output_dir = actual_output_dir,
                    cancelled  = _cancelled,
                )
            )
        except asyncio.CancelledError:
            try:
                ads_filter.save_pending_review(domain.replace(".", "_"), None)
                await asyncio.to_thread(ads_filter.save)
            except Exception:
                pass
        except Exception as e:
            logger.warning("[Finalize] Ads error: %s", e)

        try:
            await asyncio.shield(pm.flush())
        except Exception:
            pass

        icon = "✔" if completed else ("🛑" if _cancelled else "⏸")
        print(f"\n  {icon} [{tag}] {label} — {total} chapters\n", flush=True)