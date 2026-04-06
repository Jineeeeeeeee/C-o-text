"""
core/scraper.py — v19: Slim orchestrator.

v19 changes vs v18:
  SLIM-1: _format_chapter_filename + _strip_nav_edges
          → core/chapter_writer.py

  SLIM-2: _extract_story_title + _build_story_id_regex
          + _is_chapter_url + _story_id_ok
          → core/story_meta.py

  SLIM-3: _dtag
          → utils/string_helpers.domain_tag()

  ARCH-1: ctx.detected_js_heavy được xử lý tại đây (orchestrator level).

Fix L4: thay asyncio.shield() bằng _run_protected() trong finally block.

  Vấn đề: asyncio.shield() trong finally của task bị cancel không hoạt động
  như mong đợi. shield() bảo vệ inner coroutine khỏi cancel, nhưng
  `await asyncio.shield(coro)` vẫn raise CancelledError ngay lập tức
  khi task đang bị cancel — inner coro được schedule nhưng không được
  await đến completion. Kết quả: pm.flush() và _finalize_ads() có thể
  không hoàn thành, profile và ads data bị mất.

  Fix: _run_protected(coro, timeout) tạo một asyncio.Task riêng biệt
  hoàn toàn tách khỏi cancellation scope của caller, rồi await task đó
  với asyncio.wait_for(). Task riêng không bị cancel khi caller bị cancel.
  timeout đảm bảo không block vô tận nếu flush/finalize bị treo.
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
    domain_tag as _dtag,
    is_junk_page, make_fingerprint, normalize_title, truncate,
)
from utils.ads_filter     import AdsFilter
from utils.types          import ProgressDict, SiteProfile
from utils.issue_reporter import IssueReporter

from core.chapter_writer  import format_chapter_filename, strip_nav_edges
from core.story_meta      import (
    extract_story_title, build_story_id_regex,
    is_chapter_url, story_id_ok,
)
from core.session_pool    import DomainSessionPool, PlaywrightPool
from core.navigator       import find_next_url
from learning.profile_manager import ProfileManager
from ai.client            import AIRateLimiter
from ai.agents            import ai_classify_and_find, ai_find_first_chapter

from pipeline.executor    import run_chapter as pipeline_run_chapter
from pipeline.context     import context_summary

logger = logging.getLogger(__name__)

_ADS_AUTO_THRESHOLD = 10
_ADS_AI_MIN_COUNT   = 3
_ADS_FREQ_MIN_FILES = 5
MAX_EMPTY_STREAK    = 5

# Timeout cho cleanup operations trong finally block.
# Đủ dài để flush/finalize hoàn thành bình thường,
# đủ ngắn để không block Ctrl+C quá lâu.
_FLUSH_TIMEOUT_SEC    = 5.0
_FINALIZE_TIMEOUT_SEC = 30.0


# ── _run_protected ────────────────────────────────────────────────────────────

async def _run_protected(coro, timeout: float, label: str = "") -> None:
    """
    Chạy coroutine trong một Task riêng biệt với timeout.

    Fix L4: thay thế asyncio.shield() trong finally block.

    Tại sao Task riêng biệt hoạt động, shield() không:
      - asyncio.shield(coro): bảo vệ inner coro khỏi cancel, nhưng
        `await shield(...)` vẫn raise CancelledError ngay khi caller
        bị cancel → inner coro không được await đến completion.
      - asyncio.create_task(coro): tạo task hoàn toàn độc lập với
        cancellation scope của caller. Task này tiếp tục chạy ngay cả
        khi caller task bị cancel. wait_for() với timeout đảm bảo
        cleanup không block vô tận.

    Args:
        coro:    Coroutine cần chạy (flush, finalize, v.v.)
        timeout: Số giây tối đa chờ trước khi abandon
        label:   Tên để log nếu timeout/error
    """
    task = asyncio.create_task(coro)
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("[Scraper] %s timeout sau %.1fs — abandoning", label or "cleanup", timeout)
        # Không cancel task — để nó tự hoàn thành trong background nếu có thể
    except asyncio.CancelledError:
        # Caller bị cancel nhưng task con vẫn tiếp tục — đây là hành vi đúng
        logger.debug("[Scraper] %s: caller cancelled, task continues in background", label)
    except Exception as e:
        logger.warning("[Scraper] %s thất bại: %s", label or "cleanup", e)


# ── find_start_chapter ────────────────────────────────────────────────────────

async def find_start_chapter(
    start_url     : str,
    progress_path : str,
    pool          : DomainSessionPool,
    pw_pool       : PlaywrightPool,
    ai_limiter    : AIRateLimiter,
    profile       : SiteProfile,
) -> tuple[str, ProgressDict]:
    from core.navigator import detect_page_type

    progress = await load_progress(progress_path)

    if progress.get("current_url"):
        tag = _dtag(start_url)
        print(f"  [{tag}] ▶  Resume → {progress['current_url'][:65]}", flush=True)
        return progress["current_url"], progress  # type: ignore[return-value]

    if progress.get("completed"):
        raise RuntimeError("Truyện đã hoàn thành. Xóa progress file để scrape lại.")

    from core.fetch import fetch_page
    status, html = await fetch_page(start_url, pool, pw_pool)
    if is_junk_page(html, status):
        raise RuntimeError(f"Trang khởi đầu lỗi (status={status}): {start_url}")

    soup      = BeautifulSoup(html, "html.parser")
    page_type = detect_page_type(soup, start_url)

    if page_type == "chapter" and is_chapter_url(start_url, profile):
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
        if result.get("page_type") == "chapter" and is_chapter_url(start_url, profile):
            progress["start_url"] = start_url
            return start_url, progress
        for key in ("first_chapter_url", "next_url"):
            found = result.get(key)
            if found and found != start_url:
                print(f"  [{tag}] ✅ AI → {found[:65]}", flush=True)
                progress["start_url"] = start_url
                return found, progress

    raise RuntimeError(f"Không tìm được điểm bắt đầu: {start_url}")


# ── scrape_one_chapter ────────────────────────────────────────────────────────

async def scrape_one_chapter(
    url             : str,
    progress        : ProgressDict,
    progress_path   : str,
    output_dir      : str,
    pool            : DomainSessionPool,
    pw_pool         : PlaywrightPool,
    profile         : SiteProfile,
    ai_limiter      : AIRateLimiter,
    ads_filter      : AdsFilter,
    issue_reporter  : IssueReporter,
    prefetched_html : str | None = None,
) -> str | None:
    tag          = _dtag(url)
    all_visited  = set(progress.get("all_visited_urls") or [])
    fingerprints = set(progress.get("fingerprints") or [])

    if url in all_visited:
        return await _find_next_fallback(
            url, progress, progress_path, pool, pw_pool, profile,
            ai_limiter, issue_reporter=issue_reporter,
        )

    try:
        ctx = await pipeline_run_chapter(
            url             = url,
            profile         = dict(profile),
            progress        = dict(progress),
            pool            = pool,
            pw_pool         = pw_pool,
            ai_limiter      = ai_limiter,
            prefetched_html = prefetched_html,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        err_msg = str(e) or repr(e)
        ch_num  = progress.get("chapter_count", 0) + 1
        if any(kw in err_msg.lower() for kw in ("403", "captcha", "cloudflare", "blocked")):
            issue_reporter.report("BLOCKED", url, detail=err_msg[:120], chapter_num=ch_num)
        raise

    if ctx.detected_js_heavy and not profile.get("requires_playwright"):
        profile["requires_playwright"] = True  # type: ignore[typeddict-unknown-key]
        logger.info(
            "[Scraper] JS-heavy site detected: %s — profile updated (will flush at end)",
            urlparse(url).netloc,
        )

    html    = ctx.html
    content = ctx.content
    title   = ctx.title_clean or "Unknown Title"

    if not html or is_junk_page(html, ctx.status_code):
        if ctx.status_code in (403, 429):
            ch_num = progress.get("chapter_count", 0) + 1
            issue_reporter.report("BLOCKED", url, detail=f"HTTP {ctx.status_code}", chapter_num=ch_num)
        print(f"  [{tag}] 🏁 Hết truyện / junk page", flush=True)
        return None

    if not RE_CHAP_URL.search(url) and ctx.soup:
        from core.navigator import detect_page_type
        if detect_page_type(ctx.soup, url) == "index":
            print(f"  [{tag}] ⛔ INDEX page guard — dừng", flush=True)
            progress["completed"]        = True
            progress["completed_at_url"] = url
            await save_progress(progress_path, progress)
            return None

    if not content or len(content.strip()) < 100:
        ch_hint = progress.get("chapter_count", 0) + 1
        if ctx.selector_used is None:
            issue_reporter.report(
                "CONTENT_SUSPICIOUS", url,
                detail="pipeline returned 0 chars",
                chapter_num=ch_hint,
            )
        print(f"  [{tag}] ⏭  #{ch_hint:>4}: 0 chars — {truncate(url, 52)}", flush=True)
        return await _find_next_fallback(
            url, progress, progress_path, pool, pw_pool, profile,
            ai_limiter, html=html, soup=ctx.soup, issue_reporter=issue_reporter,
        )

    stripped = strip_nav_edges(content)
    if stripped and len(stripped.strip()) >= 100:
        content = stripped

    if title and re.fullmatch(r"Chapter \d+", title):
        issue_reporter.report(
            "TITLE_FALLBACK", url,
            detail=f"Title='{title}' — may be URL slug fallback",
            chapter_num=progress.get("chapter_count", 0) + 1,
        )

    content = ads_filter.filter(content, chapter_url=url)

    fp = make_fingerprint(content)
    if fp in fingerprints:
        print(f"  [{tag}] ♻  Loop nội dung — dừng", flush=True)
        return None
    fingerprints.add(fp)

    if not progress.get("story_title") and not progress.get("story_name_clean"):
        if progress.get("chapter_count", 0) == 0 and ctx.soup:
            title_tag = ctx.soup.find("title")
            if title_tag:
                raw             = title_tag.get_text(strip=True)
                story_candidate = extract_story_title(raw)
                if story_candidate:
                    progress["story_title"] = normalize_title(story_candidate)

    chapter_num = progress.get("chapter_count", 0) + 1
    filename    = format_chapter_filename(chapter_num, title, progress)
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
        f"{truncate(title, 44):<44}  {len(content):>7,}c"
        f"  [{ctx.fetch_method or '?'}→{ctx.selector_used or 'heuristic'}]",
        flush=True,
    )
    issue_reporter.mark_chapter_ok()

    next_url = ctx.next_url

    if not next_url and ctx.soup:
        next_url = find_next_url(ctx.soup, url, profile)

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
            detail="Pipeline + heuristic + AI all failed",
            chapter_num=chapter_num,
        )
        progress["completed"]        = True
        progress["completed_at_url"] = url
        await save_progress(progress_path, progress)
        print(f"  [{tag}] 🏁 Hết truyện", flush=True)
        return None

    if not story_id_ok(next_url, progress):
        print(f"  [{tag}] ⛔ Story ID guard: {next_url[:55]}", flush=True)
        return None

    if next_url in all_visited:
        print(f"  [{tag}] ♻  next_url đã thăm — dừng", flush=True)
        return None

    progress["current_url"] = next_url
    await save_progress(progress_path, progress)
    return next_url


# ── _find_next_fallback ───────────────────────────────────────────────────────

async def _find_next_fallback(
    url, progress, progress_path, pool, pw_pool, profile, ai_limiter,
    *, html=None, soup=None, issue_reporter=None,
) -> str | None:
    if soup is None or html is None:
        try:
            from core.fetch import fetch_page
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


# ── _finalize_ads ─────────────────────────────────────────────────────────────

async def _finalize_ads(
    ads_filter : AdsFilter,
    domain     : str,
    ai_limiter : AIRateLimiter,
    pm         : ProfileManager,
    output_dir : str,
    cancelled  : bool,
) -> None:
    from ai.agents import ai_verify_ads

    domain_slug      = domain.replace(".", "_")
    verified_results : dict[str, bool] = {}

    auto_candidates, ai_candidates = ads_filter.get_candidates_by_frequency(
        auto_threshold = _ADS_AUTO_THRESHOLD,
        min_count      = _ADS_AI_MIN_COUNT,
        max_results    = 20,
    )

    if auto_candidates:
        added = ads_filter.apply_verified(auto_candidates)
        for line in auto_candidates:
            verified_results[line] = True
        if added > 0:
            print(f"  [Ads] 🔒 +{added} auto-learned | {ads_filter.stats}", flush=True)
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

    ads_filter.save_pending_review(domain_slug, verified_results or None)
    await asyncio.to_thread(ads_filter.save)
    print(f"  [Ads] 💾 {ads_filter.stats}", flush=True)


# ── run_novel_task ────────────────────────────────────────────────────────────

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
    fetched_chapters   : list[tuple[str, str]] = []

    if pm.has(domain):
        existing = pm.get(domain)
        from learning.migrator import needs_migration, migrate_profile
        if needs_migration(existing):
            print(f"  [{tag}] 🔄 Migrating profile v1 → v2...", flush=True)
            migrated, requires_relearn = migrate_profile(existing)
            await pm.save_profile(domain, migrated)  # type: ignore[arg-type]
            if requires_relearn:
                print(f"  [{tag}] ⚠ Migration incomplete → force relearn", flush=True)
                del migrated["pipeline"]  # type: ignore[misc]
                await pm.save_profile(domain, migrated)  # type: ignore[arg-type]

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
        injected           = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [{tag}] [Ads] +{injected} từ profile | {ads_filter.stats}", flush=True)

    try:
        current_url, progress = await find_start_chapter(
            start_url, progress_path, pool, pw_pool, ai_limiter, profile,
        )
    except Exception as e:
        print(f"  [{tag}] ❌ Không tìm được điểm bắt đầu: {e}", flush=True)
        return

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
        sid_pattern = build_story_id_regex(current_url)
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
                            f"\n  [{tag}] ⏸  {MAX_EMPTY_STREAK} chương liên tiếp"
                            f" không có nội dung.",
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

        # Fix L4: dùng _run_protected() thay vì asyncio.shield().
        # Mỗi cleanup operation chạy trong Task riêng biệt hoàn toàn
        # tách khỏi cancellation scope của run_novel_task.
        # _finalize_ads có timeout dài hơn vì có thể gọi AI verify.
        await _run_protected(
            _finalize_ads(
                ads_filter = ads_filter,
                domain     = domain,
                ai_limiter = ai_limiter,
                pm         = pm,
                output_dir = actual_output_dir,
                cancelled  = _cancelled,
            ),
            timeout = _FINALIZE_TIMEOUT_SEC,
            label   = "finalize_ads",
        )

        # pm.flush() persist profile changes (bao gồm requires_playwright
        # từ js_heavy signal). Timeout ngắn vì chỉ là file write.
        await _run_protected(
            pm.flush(),
            timeout = _FLUSH_TIMEOUT_SEC,
            label   = "pm.flush",
        )

        icon = "✔" if completed else ("🛑" if _cancelled else "⏸")
        print(f"\n  {icon} [{tag}] {label} — {total} chapters\n", flush=True)