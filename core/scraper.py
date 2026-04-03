"""
core/scraper.py — v12: Content-selector-aware HTML filtering + auto-learn high-freq ads.

Thay đổi so với v11:
  FIX-1: Truyền content_selector vào prepare_soup() để tránh remove_selectors
         vô tình xóa nội dung bên trong content area (fanfiction.net bug).
  FIX-2: _finalize_ads() phân tầng: ≥5× auto-add, 2–4× AI verify.
  FMT:   Domain tag trên mọi dòng output → dễ đọc khi nhiều task song song.
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
from core.fetch           import fetch_page
from core.html_filter     import prepare_soup
from core.extractor       import extract_chapter
from core.navigator       import find_next_url, detect_page_type
from core.session_pool    import DomainSessionPool, PlaywrightPool
from learning.profile_manager import ProfileManager
from ai.client            import AIRateLimiter
from ai.agents            import ai_classify_and_find, ai_find_first_chapter

logger = logging.getLogger(__name__)

# Số lần lặp tối thiểu để auto-add ads mà không cần AI
_ADS_AUTO_THRESHOLD = 5


# ── Domain tag helper ─────────────────────────────────────────────────────────

def _dtag(url_or_domain: str) -> str:
    """
    Trả về short domain label cố định 12 ký tự để align terminal output.
    VD: "novelfire.net" → "novelfire   "
        "www.royalroad.com" → "royalroad   "
        "www.fanfiction.net" → "fanfiction  "
    """
    if url_or_domain.startswith("http"):
        netloc = urlparse(url_or_domain).netloc.lower()
    else:
        netloc = url_or_domain.lower()
    name = netloc.replace("www.", "").split(".")[0]
    return f"{name[:12]:<12}"


# ── Nav-edge strip ────────────────────────────────────────────────────────────

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


# ── Story ID guard ────────────────────────────────────────────────────────────

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


# ── Find start chapter ────────────────────────────────────────────────────────

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
    tag          = _dtag(url)
    all_visited  : set[str] = set(progress.get("all_visited_urls") or [])
    fingerprints : set[str] = set(progress.get("fingerprints") or [])

    if url in all_visited:
        return await _find_next_and_save(url, progress, progress_path, pool, pw_pool, profile, ai_limiter)

    status, html = await fetch_page(url, pool, pw_pool)
    if is_junk_page(html, status):
        print(f"  [{tag}] 🏁 Hết truyện / junk page", flush=True)
        return None

    # FIX-1: truyền content_selector để bảo vệ content area khỏi remove_selectors
    soup = await asyncio.to_thread(
        prepare_soup, html,
        profile.get("remove_selectors"),
        profile.get("content_selector"),
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
        print(f"  [{tag}] ⏭  #{ch_hint:>4}: 0 chars — {truncate(url, 52)}", flush=True)
        return await _find_next_and_save(url, progress, progress_path, pool, pw_pool, profile, ai_limiter)

    stripped = _strip_nav_edges(content)
    if stripped and len(stripped.strip()) >= 100:
        content = stripped

    before_len = len(content)
    content    = ads_filter.filter(content, chapter_url=url)
    removed    = before_len - len(content)
    if removed > 100:
        print(f"  [{tag}] [Ads] -{removed:,}c", flush=True)

    fp = make_fingerprint(content)
    if fp in fingerprints:
        print(f"  [{tag}] ♻  Loop nội dung — dừng", flush=True)
        return None
    fingerprints.add(fp)

    if not progress.get("story_title") and progress.get("chapter_count", 0) == 0:
        title_tag = soup.find("title")
        if title_tag:
            raw = title_tag.get_text(strip=True)
            m   = re.search(r"[\|–—]", raw)
            if m:
                story_candidate = normalize_title(raw[: m.start()].strip())
                if len(story_candidate) > 3:
                    progress["story_title"] = story_candidate

    chapter_num = progress.get("chapter_count", 0) + 1
    filename    = f"{chapter_num:04d}_{slugify_filename(title, max_len=60)}.md"
    filepath    = os.path.join(output_dir, filename)
    await write_markdown(filepath, f"# {title}\n\n{content}\n")

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

    next_url = find_next_url(soup, url, profile)
    if not next_url:
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
) -> str | None:
    try:
        _, html = await fetch_page(url, pool, pw_pool)
    except Exception:
        return None
    soup     = BeautifulSoup(html, "html.parser")
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


# ── Ads finalization ──────────────────────────────────────────────────────────

async def _finalize_ads(
    ads_filter : AdsFilter,
    domain     : str,
    ai_limiter : AIRateLimiter,
    pm         : ProfileManager,
    cancelled  : bool,
) -> None:
    """
    Finalize ads sau khi scrape xong / bị cancel.

    Phân tầng xử lý:
      Tier 1 — Auto-add (count ≥ _ADS_AUTO_THRESHOLD):
        Lặp đủ nhiều → chắc chắn là watermark cố định → thêm ngay, không cần AI.
      Tier 2 — AI verify (2 ≤ count < threshold):
        Không chắc → gửi AI phân biệt ads thật vs false positive.
        Chỉ chạy khi NOT cancelled.
    """
    from ai.agents import ai_verify_ads

    domain_slug      = domain.replace(".", "_")
    verified_results : dict[str, bool] = {}

    auto_candidates, ai_candidates = ads_filter.get_candidates_by_frequency(
        auto_threshold = _ADS_AUTO_THRESHOLD,
        min_count      = 2,
        max_results    = 20,
    )

    # ── Tier 1: Auto-add high-frequency ──────────────────────────────────────
    if auto_candidates:
        added = ads_filter.apply_verified(auto_candidates)
        for line in auto_candidates:
            verified_results[line] = True
        if added > 0:
            print(
                f"  [Ads] 🔒 +{added} auto-learned "
                f"(≥{_ADS_AUTO_THRESHOLD}× /phiên) | {ads_filter.stats}",
                flush=True,
            )
            await pm.add_ads_to_profile(domain, auto_candidates)

    # ── Tier 2: AI verify low-frequency ──────────────────────────────────────
    if not cancelled and ai_candidates:
        print(
            f"  [Ads] 🤖 AI xác nhận {len(ai_candidates)} dòng không rõ...",
            flush=True,
        )
        try:
            confirmed     = await ai_verify_ads(ai_candidates, domain, ai_limiter)
            confirmed_set = set(confirmed)
            for line in ai_candidates:
                verified_results[line] = line in confirmed_set

            if confirmed:
                added = ads_filter.apply_verified(confirmed)
                if added > 0:
                    print(
                        f"  [Ads] ✅ +{added} AI-confirmed | {ads_filter.stats}",
                        flush=True,
                    )
                    await pm.add_ads_to_profile(domain, confirmed)
                fp_n = len(ai_candidates) - len(confirmed)
                if fp_n > 0:
                    print(f"  [Ads] ℹ️  {fp_n} false positive — bỏ qua", flush=True)
            else:
                print(f"  [Ads] ℹ️  0 keyword mới từ AI", flush=True)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[Ads] AI verify thất bại: %s", e)

    # ── Save review file + DB ─────────────────────────────────────────────────
    review_path = ads_filter.save_pending_review(
        domain_slug,
        verified_results if verified_results else None,
    )
    if review_path:
        summary    = ads_filter.get_session_summary()
        total      = len(summary)
        confirmed_n = sum(1 for v in verified_results.values() if v)
        icon       = "🛑" if cancelled else "📋"
        print(
            f"  [Ads] {icon} {total} dòng lọc"
            + (f" | {confirmed_n} xác nhận" if confirmed_n else "")
            + f" → {review_path}",
            flush=True,
        )

    await asyncio.to_thread(ads_filter.save)
    print(f"  [Ads] 💾 {ads_filter.stats}", flush=True)


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
    os.makedirs(output_dir, exist_ok=True)
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    ads_filter = AdsFilter.load(domain=domain)

    # ── Phase 1 → 2: Learning (nếu cần) ──────────────────────────────────────
    if not pm.has(domain) or not pm.is_profile_fresh(domain):
        if os.path.exists(progress_path):
            try:
                os.remove(progress_path)
                print(f"  [{tag}] 🗑  Cleared old progress", flush=True)
            except Exception as e:
                logger.warning("[Learn] Failed to clear progress: %s", e)

        from learning.phase import run_learning_phase
        profile = await run_learning_phase(start_url, pool, pw_pool, pm, ai_limiter)
        if profile is None:
            print(f"  [{tag}] ❌ Learning Phase thất bại. Bỏ qua.", flush=True)
            return

        injected = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [{tag}] [Ads] +{injected} từ profile | {ads_filter.stats}", flush=True)

        print(f"\n  [{tag}] 🔄 Reset progress → scrape lại từ Ch.1...\n", flush=True)
        await save_progress(progress_path, {
            "current_url"     : None,
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
        print(f"  [{tag}] 📂 {pm.summary(domain)}", flush=True)
        profile  = pm.get(domain)
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

    if not progress.get("story_id_locked"):
        sid_pattern = _build_story_id_regex(current_url)
        if sid_pattern:
            progress["story_id_regex"]  = sid_pattern
            progress["story_id_locked"] = True
            await save_progress(progress_path, progress)
            logger.debug("[StoryID] Locked: %s", sid_pattern)

    story_label = progress.get("story_title") or urlparse(start_url).netloc
    print(f"\n{'─'*62}", flush=True)
    print(f"  🚀 [{tag}] {story_label}", flush=True)
    print(f"{'─'*62}", flush=True)

    consecutive_errors   = 0
    consecutive_timeouts = 0
    _cancelled           = False

    try:
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
        label     = progress.get("story_title") or start_url[:50]

        try:
            await asyncio.shield(
                _finalize_ads(ads_filter, domain, ai_limiter, pm, _cancelled)
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