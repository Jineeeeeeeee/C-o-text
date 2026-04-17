"""
learning/phase.py — Learning Phase orchestrator (v5).

Batch B: Xóa _build_pipeline_from_ai().
  Trước: Hàm này tạo PipelineConfig object → to_dict() → lưu vào profile["pipeline"].
         PipelineRunner.from_profile() lại deserialize từ profile["pipeline"].
         Roundtrip vô nghĩa — profile đã có flat fields (content_selector, v.v.).
  Sau:   _build_final_profile() nhận requires_playwright bool trực tiếp.
         _detect_js_heavy() là helper nhỏ thay thế phần JS detection.
         profile["pipeline"] không còn được tạo ra.
         PipelineRunner đọc flat fields trực tiếp.

Fix P3-17: đổi curl_htmls: list[str] → curl_html_ch1: str | None.
FIX-REQUIRESPW: _build_final_profile() set requires_playwright=True khi JS-heavy.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import LEARNING_CHAPTERS, get_delay, RE_CHAP_URL, JS_CONTENT_RATIO, JS_MIN_DIFF_CHARS
from utils.types import SiteProfile
from utils.string_helpers import is_junk_page
from core.fetch import fetch_page
from core.session_pool import DomainSessionPool, PlaywrightPool
from core.navigator import find_next_url
from learning.profile_manager import ProfileManager
from ai.client import AIRateLimiter
from ai.agents import ai_classify_and_find, ai_find_first_chapter

logger = logging.getLogger(__name__)


async def run_learning_phase(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
) -> tuple[SiteProfile, list[str], list[tuple[str, str]]] | None:
    """
    Chạy Learning Phase đầy đủ (8 AI calls).

    Returns:
        (profile, sample_raw_titles, fetched_chapters) hoặc None nếu thất bại.
    """
    from utils.string_helpers import domain_tag as _dtag
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    # CAO_FAST_LEARNING là no-op sau Batch A (optimizer đã bỏ)
    if os.getenv("CAO_FAST_LEARNING") == "1":
        print(f"  [{tag}] ℹ CAO_FAST_LEARNING set nhưng optimizer đã bỏ — ignored", flush=True)

    print(f"\n{'═'*62}", flush=True)
    print(f"  🎓 Deep Learning: {domain}", flush=True)
    print(f"  📚 Fetching {LEARNING_CHAPTERS} chapters...", flush=True)
    print(f"{'═'*62}", flush=True)

    # ── 1. Fetch chapters ─────────────────────────────────────────────────────
    chapters, curl_html_ch1 = await _fetch_chapters(
        start_url, pool, pw_pool, pm, ai_limiter, domain,
    )

    if len(chapters) < 4:
        print(
            f"  [{tag}] ✗ Chỉ fetch được {len(chapters)}/{LEARNING_CHAPTERS} chapters — không đủ để học.",
            flush=True,
        )
        return None

    n = len(chapters)
    print(f"  [{tag}] ✓ Fetched {n}/{LEARNING_CHAPTERS} chapters\n", flush=True)

    # ── 2. 8 AI calls (học selectors) ────────────────────────────────────────
    from learning.phase_ai import run_10_ai_calls_internal
    ai_profile = await run_10_ai_calls_internal(chapters, domain, ai_limiter)

    if ai_profile is None:
        print(f"  [{tag}] ⚠ AI calls thất bại — dùng empty profile", flush=True)
        ai_profile = {}

    # ── 3. Detect JS-heavy, tính requires_playwright ──────────────────────────
    requires_pw = _detect_js_heavy(ai_profile, curl_html_ch1, chapters)
    if requires_pw and not ai_profile.get("requires_playwright", False):
        logger.info("[Phase] %s: requires_playwright=True (JS-heavy detected)", domain)
    print(
        f"  [{tag}]    content={ai_profile.get('content_selector')!r} "
        f"nav_type={ai_profile.get('nav_type')!r} "
        f"requires_pw={requires_pw}",
        flush=True,
    )

    # ── 4. Build final profile ────────────────────────────────────────────────
    profile = _build_final_profile(domain, ai_profile, requires_pw, n, chapters)
    await pm.save_profile(domain, profile)

    _print_summary(tag, profile)

    from learning.naming import get_raw_title_from_html
    sample_titles: list[str] = [
        t for t in (get_raw_title_from_html(html) for _, html in chapters) if t
    ]

    return profile, sample_titles, chapters


# ── JS-heavy detection ────────────────────────────────────────────────────────

def _detect_js_heavy(
    ai_profile    : dict,
    curl_html_ch1 : str | None,
    chapters      : list[tuple[str, str]],
) -> bool:
    """
    Xác định site có cần Playwright không.

    Logic:
        1. AI flag requires_playwright = True → True
        2. So sánh curl vs playwright text length cho Ch.1:
           nếu pw_len > curl_len × JS_CONTENT_RATIO AND diff > JS_MIN_DIFF_CHARS
           → True (JS-heavy site, content only visible after JS execution)

    Replaces _build_pipeline_from_ai() JS detection logic (Batch B).
    """
    if ai_profile.get("requires_playwright", False):
        return True

    if not curl_html_ch1 or not chapters:
        return False

    try:
        pw_html  = chapters[0][1]
        curl_len = len(BeautifulSoup(curl_html_ch1, "html.parser").get_text())
        pw_len   = len(BeautifulSoup(pw_html,       "html.parser").get_text())
        if pw_len > curl_len * JS_CONTENT_RATIO and (pw_len - curl_len) > JS_MIN_DIFF_CHARS:
            logger.info(
                "[Phase] JS-heavy: curl=%d pw=%d ratio=%.1f",
                curl_len, pw_len, pw_len / max(curl_len, 1),
            )
            return True
    except Exception:
        pass

    return False


# ── Chapter fetching ──────────────────────────────────────────────────────────

async def _fetch_chapters(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
    domain     : str,
) -> tuple[list[tuple[str, str]], str | None]:
    """
    Fetch LEARNING_CHAPTERS chapters.

    Returns:
        (chapters, curl_html_ch1)
        chapters      = [(url, playwright_html)]
        curl_html_ch1 = str | None — curl HTML của Ch.1, dùng để detect JS-heavy
    """
    from utils.string_helpers import domain_tag as _dtag
    tag = _dtag(domain)

    chapters     : list[tuple[str, str]] = []
    curl_html_ch1: str | None            = None
    current_url   = start_url

    if not RE_CHAP_URL.search(start_url):
        print(f"  [{tag}] 📋 Index page → tìm Chapter 1...", flush=True)
        try:
            status, index_html = await pw_pool.fetch(start_url)
            if not is_junk_page(index_html, status):
                first_url = await ai_find_first_chapter(index_html, start_url, ai_limiter)
                if first_url and first_url != start_url:
                    print(f"  [{tag}] ✅ Chapter 1: {first_url[:65]}", flush=True)
                    current_url = first_url
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [{tag}] ⚠ Index detection thất bại: {e}", flush=True)

    temp_profile: SiteProfile = pm.get(domain)  # type: ignore[assignment]

    for i in range(LEARNING_CHAPTERS):
        if not current_url:
            break

        print(f"  [{tag}] Fetch Ch.{i+1:>2}/{LEARNING_CHAPTERS} → {current_url[:60]}", flush=True)

        try:
            if i == 0:
                status, html = await pw_pool.fetch(current_url)
                try:
                    _, curl_html_ch1 = await pool.fetch(current_url)
                except Exception:
                    curl_html_ch1 = None
            else:
                status, html = await fetch_page(current_url, pool, pw_pool)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [{tag}] ⚠ Fetch Ch.{i+1} thất bại: {e}", flush=True)
            break

        if is_junk_page(html, status):
            print(f"  [{tag}] ⚠ Ch.{i+1} junk page (status={status})", flush=True)
            break

        chapters.append((current_url, html))

        if i < LEARNING_CHAPTERS - 1:
            soup     = BeautifulSoup(html, "html.parser")
            next_url = find_next_url(soup, current_url, temp_profile)

            if not next_url:
                print(f"  [{tag}] ⚠ Heuristic nav thất bại Ch.{i+1} → AI fallback...", flush=True)
                try:
                    ai_nav = await ai_classify_and_find(html, current_url, ai_limiter)
                    if ai_nav:
                        next_url = ai_nav.get("next_url")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[%s] AI nav thất bại: %s", tag, e)

            if not next_url:
                print(f"  [{tag}] ⚠ Không tìm được next URL sau Ch.{i+1}", flush=True)
                break

            current_url = next_url
            await asyncio.sleep(get_delay(current_url))

    return chapters, curl_html_ch1


# ── Profile builder ───────────────────────────────────────────────────────────

def _build_final_profile(
    domain           : str,
    ai_profile       : dict,
    requires_playwright: bool,
    n_chapters       : int,
    chapters         : list[tuple[str, str]],
) -> SiteProfile:
    """
    Build SiteProfile từ AI results và JS-heavy detection.

    Batch B: Không còn nhận pipeline_config param.
    Không tạo profile["pipeline"] — PipelineRunner đọc flat fields trực tiếp.
    """
    urls = [url for url, _ in chapters]
    fr   = ai_profile.get("formatting_rules") or {}

    profile: SiteProfile = {
        "domain"               : domain,
        "last_learned"         : datetime.now(timezone.utc).isoformat(),
        "confidence"           : float(ai_profile.get("confidence", 0.7)),
        "profile_version"      : 2,
        "content_selector"     : ai_profile.get("content_selector"),
        "next_selector"        : ai_profile.get("next_selector"),
        "title_selector"       : ai_profile.get("title_selector") or ai_profile.get("chapter_title_selector"),
        "remove_selectors"     : ai_profile.get("remove_selectors", []),
        "nav_type"             : ai_profile.get("nav_type"),
        "chapter_url_pattern"  : ai_profile.get("chapter_url_pattern"),
        "requires_playwright"  : requires_playwright,
        "formatting_rules"     : fr,
        "ads_keywords_learned" : list(ai_profile.get("ads_keywords_learned") or []),
        "learned_chapters"     : list(range(1, n_chapters + 1)),
        "sample_urls"          : urls,
    }

    if ai_profile.get("uncertain_fields"):
        profile["uncertain_fields"] = ai_profile["uncertain_fields"]  # type: ignore[typeddict-unknown-key]

    return profile  # type: ignore[return-value]


def _print_summary(tag: str, profile: SiteProfile) -> None:
    fr = profile.get("formatting_rules") or {}
    print(
        f"\n  [{tag}] ✅ Profile saved!\n"
        f"     confidence       = {profile.get('confidence', 0):.2f}\n"
        f"     content_selector = {profile.get('content_selector')!r}\n"
        f"     title_selector   = {profile.get('title_selector')!r}\n"
        f"     next_selector    = {profile.get('next_selector')!r}\n"
        f"     remove           = {profile.get('remove_selectors', [])}\n"
        f"     nav_type         = {profile.get('nav_type')!r}\n"
        f"     requires_pw      = {profile.get('requires_playwright', False)}\n"
        f"     tables/math      = {fr.get('tables', False)} / {fr.get('math_support', False)}\n"
        f"     ads_kw           = {len(profile.get('ads_keywords_learned', []))}",
        flush=True,
    )
    if profile.get("uncertain_fields"):
        print(f"     ⚠ uncertain: {profile['uncertain_fields']}", flush=True)
    print(f"{'═'*62}\n", flush=True)