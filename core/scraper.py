"""
core/scraper.py — v10: Profile-aware start detection + Story ID guard.

Thay đổi so với v9:
  Fix #2: find_start_chapter() dùng profile.chapter_url_pattern để detect
          chapter URL chính xác hơn RE_CHAP_URL (cover sites có URL format lạ)
  Fix #3: _build_story_id_regex() + lock story ID sau khi xác định start URL
          ngăn scraper đi lạc sang story khác khi next_url bị redirect sai
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


# ── Nav-edge strip ────────────────────────────────────────────────────────────

_RE_WORD_COUNT = re.compile(
    r"^\[\s*[\d,.\s]+words?\s*\]$|^\[\s*\.+\s*words?\s*\]$",
    re.IGNORECASE,
)
_NAV_EDGE_SCAN = 7


def _strip_nav_edges(text: str) -> str:
    """
    Xóa nav header/footer lặp lại ở đầu/cuối content block.
    Chỉ strip LIÊN TIẾP từ đầu/cuối — break ngay khi gặp dòng content thật.
    """
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
    """
    Trích pattern định danh story từ URL chapter để lock navigation.
    Ngăn scraper đi lạc sang story khác khi next_url bị redirect sai.

    Logic: tìm segment path đầu tiên chứa numeric ID (story ID),
    dùng phần path trước + ID đó làm anchor regex.

    Examples:
        https://www.fanfiction.net/s/12345678/3/Title  → r'/s/12345678/'
        https://www.royalroad.com/fiction/55418/slug/chapter/123
                                                        → r'/fiction/55418/'
        https://novelfire.net/novel-name/chapter-1      → None (không có numeric ID)

    Returns:
        Escaped regex string hoặc None nếu không extract được.
    """
    try:
        path     = urlparse(url).path  # e.g. /fiction/55418/slug/chapter/123
        segments = [s for s in path.split("/") if s]

        # fanfiction.net: /s/{story_id}/{chap_num}/...
        if len(segments) >= 3 and segments[0] == "s" and segments[1].isdigit():
            story_path = f"/s/{segments[1]}/"
            return re.escape(story_path)

        # Generic: tìm segment /word/{numeric_id}/ đầu tiên
        # e.g. /fiction/55418/ hoặc /series/123/ hoặc /novel/456/
        for i, seg in enumerate(segments):
            if seg.isdigit() and i > 0:
                story_path = "/" + "/".join(segments[: i + 1]) + "/"
                return re.escape(story_path)

    except Exception:
        pass
    return None


def _is_chapter_url(url: str, profile: SiteProfile) -> bool:
    """
    Kiểm tra URL có phải chapter URL không.
    Ưu tiên profile.chapter_url_pattern nếu có (chính xác hơn RE_CHAP_URL).
    """
    # Fast path: thử profile pattern trước
    pattern = profile.get("chapter_url_pattern")
    if pattern:
        try:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        except re.error:
            pass  # Pattern lỗi → fallback RE_CHAP_URL

    # Fallback: regex chung
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
    """
    Xác định URL chapter đầu tiên cần scrape. Resume từ progress nếu có.

    Fix #2: Dùng profile.chapter_url_pattern để detect chapter URL chính xác hơn,
    tránh trường hợp sites có URL format không khớp RE_CHAP_URL (ví dụ slug-only).
    """
    progress = await load_progress(progress_path)

    if progress.get("current_url"):
        print(f"  [Resume] ▶ {progress['current_url'][:70]}", flush=True)
        return progress["current_url"], progress  # type: ignore[return-value]

    if progress.get("completed"):
        raise RuntimeError("Truyện đã hoàn thành. Xóa progress file để scrape lại.")

    status, html = await fetch_page(start_url, pool, pw_pool)
    if is_junk_page(html, status):
        raise RuntimeError(f"Trang khởi đầu lỗi (status={status}): {start_url}")

    soup      = BeautifulSoup(html, "html.parser")
    page_type = detect_page_type(soup, start_url)

    # Fix #2: dùng _is_chapter_url() thay vì RE_CHAP_URL trực tiếp
    # → cover sites có chapter_url_pattern đặc biệt không match RE_CHAP_URL
    if page_type == "chapter" and _is_chapter_url(start_url, profile):
        print(f"  [Start] 📖 Chapter page: {start_url[:70]}", flush=True)
        progress["start_url"] = start_url
        return start_url, progress

    print(f"  [Start] 📋 Index/unknown page → tìm Chapter 1...", flush=True)
    first_url = await ai_find_first_chapter(html, start_url, ai_limiter)
    if first_url and first_url != start_url:
        print(f"  [Start] ✅ Chapter 1: {first_url[:70]}", flush=True)
        progress["start_url"] = start_url
        return first_url, progress

    result = await ai_classify_and_find(html, start_url, ai_limiter)
    if result:
        # Fix #2: AI confirm chapter + profile-aware URL check
        if result.get("page_type") == "chapter" and _is_chapter_url(start_url, profile):
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
    Returns: next_url nếu thành công, None nếu hết truyện hoặc lỗi không phục hồi.
    """
    all_visited : set[str] = set(progress.get("all_visited_urls") or [])
    fingerprints: set[str] = set(progress.get("fingerprints") or [])

    if url in all_visited:
        print(f"  [Skip] ⏭ Đã thăm: {url[:60]}", flush=True)
        return await _find_next_and_save(url, progress, progress_path, pool, pw_pool, profile, ai_limiter)

    status, html = await fetch_page(url, pool, pw_pool)
    if is_junk_page(html, status):
        print(f"  [End] 🏁 Junk/hết truyện: {url[:60]}", flush=True)
        return None

    soup = await asyncio.to_thread(prepare_soup, html, profile.get("remove_selectors"))

    if not RE_CHAP_URL.search(url):
        page_type = detect_page_type(soup, url)
        if page_type == "index":
            print(f"  ⚠️  [Guard] INDEX page — dừng: {url[:70]}", flush=True)
            progress["completed"]        = True
            progress["completed_at_url"] = url
            await save_progress(progress_path, progress)
            return None

    content, title, selector = await asyncio.to_thread(extract_chapter, soup, url, profile)

    if not content or len(content.strip()) < 100:
        print(f"  [Skip] {len((content or '').strip())} chars — bỏ qua: {url[:60]}", flush=True)
        return await _find_next_and_save(url, progress, progress_path, pool, pw_pool, profile, ai_limiter)

    stripped = _strip_nav_edges(content)
    if stripped and len(stripped.strip()) >= 100:
        content = stripped

    # ── Ads filter — truyền url để log context ────────────────────────────────
    before_len = len(content)
    content    = ads_filter.filter(content, chapter_url=url)
    removed    = before_len - len(content)
    if removed > 50:
        print(f"  [Ads] 🧹 -{removed} chars", flush=True)

    fp = make_fingerprint(content)
    if fp in fingerprints:
        print(f"  [Loop] ♻ Lặp nội dung: {url[:60]}", flush=True)
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
        f"  ✅ Ch.{chapter_num:>4}: "
        f"{truncate(title, 45):<45} | {len(content):>5} chars",
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
        print(f"  [End] 🏁 Hết truyện.", flush=True)
        return None

    # Fix #3: story ID guard — chặn URL lạc sang story khác
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
    url, progress, progress_path, pool, pw_pool, profile, ai_limiter,
) -> str | None:
    """Helper: tìm next URL cho trang đã bỏ qua."""
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
    Finalize sau khi scrape xong (hoặc bị cancel):
      1. AI verify unknown candidates (chỉ khi NOT cancelled)
      2. Apply confirmed keywords vào domain bucket + profile
      3. Lưu pending review JSON (với kết quả verify nếu có)
      4. Lưu ads DB
    """
    from ai.agents import ai_verify_ads

    domain_slug      = domain.replace(".", "_")
    verified_results : dict[str, bool] = {}

    if not cancelled:
        candidates = ads_filter.get_unknown_candidates(min_count=2, max_results=20)
        if candidates:
            print(
                f"  [Ads] 🤖 AI xác nhận {len(candidates)} dòng lọc chưa biết...",
                flush=True,
            )
            try:
                confirmed = await ai_verify_ads(candidates, domain, ai_limiter)
                confirmed_set = set(confirmed)
                for line in candidates:
                    verified_results[line] = line in confirmed_set

                if confirmed:
                    added = ads_filter.apply_verified(confirmed)
                    if added > 0:
                        print(
                            f"  [Ads] ✅ +{added} keyword được xác nhận ({ads_filter.stats})",
                            flush=True,
                        )
                        await pm.add_ads_to_profile(domain, confirmed)
                else:
                    print(f"  [Ads] ℹ️  Không có keyword mới được xác nhận.", flush=True)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[Ads] AI verify thất bại: %s", e)

    review_path = ads_filter.save_pending_review(
        domain_slug,
        verified_results if verified_results else None,
    )
    if review_path:
        summary     = ads_filter.get_session_summary()
        total_lines = len(summary)
        verified_n  = sum(1 for v in verified_results.values() if v)
        if cancelled:
            print(
                f"  [Ads] 📋 {total_lines} dòng lọc → pending review: {review_path}",
                flush=True,
            )
        else:
            print(
                f"  [Ads] 📋 {total_lines} dòng lọc | {verified_n} xác nhận ads "
                f"→ {review_path}",
                flush=True,
            )

    await asyncio.to_thread(ads_filter.save)
    print(f"  [Ads] 💾 {ads_filter.stats} saved", flush=True)


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
      2. Reset progress → scrape lại từ đầu với profile đầy đủ
      3. Find start chapter + lock story ID (Fix #3)
      4. Full Scrape Mode loop
      5. finally: luôn chạy _finalize_ads + pm.flush()
    """
    os.makedirs(output_dir, exist_ok=True)
    domain = urlparse(start_url).netloc.lower()

    ads_filter = AdsFilter.load(domain=domain)

    # ── Phase 1 → 2: Learning (nếu cần) ──────────────────────────────────────
    if not pm.has(domain) or not pm.is_profile_fresh(domain):
        from learning.phase import run_learning_phase
        profile = await run_learning_phase(start_url, pool, pw_pool, pm, ai_limiter)
        if profile is None:
            print(f"  [ERR] Learning Phase thất bại cho {domain}. Bỏ qua.", flush=True)
            return

        injected = ads_filter.inject_from_profile(profile)
        if injected > 0:
            print(f"  [Ads] +{injected} keywords từ profile ({ads_filter.stats})", flush=True)

        print(f"\n🔄 Reset progress → scrape lại từ Chapter 1 với profile hoàn chỉnh...", flush=True)
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
        print(f"  [Profile] 📂 {pm.summary(domain)}", flush=True)
        profile  = pm.get(domain)
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

    # Fix #3: Lock story ID ngay sau khi xác định được start chapter URL.
    # Chỉ set khi chưa có (tránh override khi resume — đã locked từ lần trước).
    if not progress.get("story_id_locked"):
        sid_pattern = _build_story_id_regex(current_url)
        if sid_pattern:
            progress["story_id_regex"]  = sid_pattern
            progress["story_id_locked"] = True
            await save_progress(progress_path, progress)
            logger.debug("[StoryID] Locked pattern: %s", sid_pattern)

    story_label = progress.get("story_title") or start_url[:50]
    print(f"\n🚀 {story_label}", flush=True)

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
                print(f"  [Cancel] 🛑 Progress đã lưu.", flush=True)
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
                import traceback
                print(
                    f"  [ERR #{consecutive_errors}] {type(e).__name__}: {e}\n"
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

        status = "✔" if completed else ("🛑" if _cancelled else "⏸")
        print(f"\n{status} {label} — {total} chapters", flush=True)