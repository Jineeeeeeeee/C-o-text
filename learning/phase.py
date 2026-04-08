"""
learning/phase.py — Learning Phase orchestrator (v3).

Fix P1-5: xóa 11 dead imports từ ai.agents.
Fix P1-6: xóa wrapper _run_10_ai_calls(), gọi thẳng run_10_ai_calls_internal().
Fix P3-17: đổi curl_htmls: list[str] → curl_html_ch1: str | None.
  Trước: curl_htmls là list[str] nhưng chỉ index 0 có data thật,
  còn lại là "" placeholder. Tên ngụ ý list đầy đủ → developer hiểu sai.
  Sau: curl_html_ch1: str | None — tên nói rõ đây là curl HTML của Ch.1 dùng
  để detect JS-heavy. Optimizer nhận str | None thay vì list[str].
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import LEARNING_CHAPTERS, get_delay, RE_CHAP_URL
from utils.types import SiteProfile
from utils.string_helpers import is_junk_page
from core.fetch import fetch_page
from core.session_pool import DomainSessionPool, PlaywrightPool
from core.navigator import find_next_url
from learning.profile_manager import ProfileManager
from learning.optimizer import run_optimizer
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
    Chạy Learning Phase đầy đủ (10 AI calls + optimizer).

    Returns:
        (profile, sample_raw_titles, fetched_chapters) hoặc None nếu thất bại.
    """
    from utils.string_helpers import domain_tag as _dtag
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    fast_learning = os.getenv("CAO_FAST_LEARNING") == "1"
    if fast_learning:
        print(f"  [{tag}] ⚡ Fast-learning mode: optimizer sẽ bị skip", flush=True)

    print(f"\n{'═'*62}", flush=True)
    print(f"  🎓 Deep Learning: {domain}", flush=True)
    print(f"  📚 Fetching {LEARNING_CHAPTERS} chapters...", flush=True)
    print(f"{'═'*62}", flush=True)

    # ── 1. Fetch chapters ─────────────────────────────────────────────────────
    # Fix P3-17: _fetch_chapters trả về (chapters, curl_html_ch1) thay vì list
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

    # ── 2. 10 AI calls (học selectors) ───────────────────────────────────────
    from learning.phase_ai import run_10_ai_calls_internal
    ai_profile = await run_10_ai_calls_internal(chapters, domain, ai_limiter)

    if ai_profile is None:
        print(f"  [{tag}] ⚠ 10 AI calls thất bại — dùng empty profile cho optimizer", flush=True)
        ai_profile = {}

    # ── 3. Optimizer hoặc fast-learning path ─────────────────────────────────
    if fast_learning:
        print(f"  [{tag}] ⚡ Fast-learning: skip optimizer, dùng default pipeline", flush=True)
        from pipeline.base import PipelineConfig
        from learning.optimizer import _merge_ai_selectors
        pipeline_config = PipelineConfig.default_for_domain(domain)
        _merge_ai_selectors(pipeline_config, ai_profile)
        pipeline_config.notes = "fast_learning_default"
        pipeline_config.score = float(ai_profile.get("confidence", 0.5))
    else:
        print(f"\n  [{tag}] 🔧 Pipeline Optimizer...", flush=True)
        try:
            # Fix P3-17: truyền curl_html_ch1 thay vì curl_htmls list
            pipeline_config = await run_optimizer(
                domain           = domain,
                chapters         = chapters,
                existing_profile = ai_profile,
                pool             = pool,
                pw_pool          = pw_pool,
                ai_limiter       = ai_limiter,
                curl_html_ch1    = curl_html_ch1,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[Phase] Optimizer thất bại: %s — dùng default pipeline", e)
            from pipeline.base import PipelineConfig
            from learning.optimizer import _merge_ai_selectors
            pipeline_config = PipelineConfig.default_for_domain(domain)
            _merge_ai_selectors(pipeline_config, ai_profile)

    # ── 4. Build final profile ────────────────────────────────────────────────
    profile = _build_final_profile(domain, ai_profile, pipeline_config, n, chapters)
    await pm.save_profile(domain, profile)

    _print_summary(tag, profile)

    from learning.naming import get_raw_title_from_html
    sample_titles: list[str] = [
        t for t in (get_raw_title_from_html(html) for _, html in chapters) if t
    ]

    return profile, sample_titles, chapters


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

    Fix P3-17: trả về (chapters, curl_html_ch1) thay vì (chapters, curl_htmls: list).
    curl_html_ch1 là curl HTML của Ch.1 — dùng để detect JS-heavy trong optimizer.
    Chỉ Ch.1 được fetch bằng cả Playwright và curl; các chapter sau chỉ cần 1 lần.

    Returns:
        (chapters, curl_html_ch1)
        chapters      = [(url, playwright_html)]
        curl_html_ch1 = str | None — curl HTML của Ch.1, None nếu curl thất bại
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
                # Ch.1: Playwright để đảm bảo full render
                status, html = await pw_pool.fetch(current_url)
                # Fetch curl riêng để detect JS-heavy (Fix P3-17: lưu trực tiếp)
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
    domain         : str,
    ai_profile     : dict,
    pipeline_config,
    n_chapters     : int,
    chapters       : list[tuple[str, str]],
) -> SiteProfile:
    urls = [url for url, _ in chapters]
    fr   = ai_profile.get("formatting_rules") or {}

    profile: SiteProfile = {
        "domain"               : domain,
        "last_learned"         : datetime.now(timezone.utc).isoformat(),
        "confidence"           : ai_profile.get("confidence", pipeline_config.score),
        "content_selector"     : ai_profile.get("content_selector"),
        "next_selector"        : ai_profile.get("next_selector"),
        "title_selector"       : ai_profile.get("title_selector") or ai_profile.get("chapter_title_selector"),
        "remove_selectors"     : ai_profile.get("remove_selectors", []),
        "nav_type"             : ai_profile.get("nav_type"),
        "chapter_url_pattern"  : ai_profile.get("chapter_url_pattern"),
        "requires_playwright"  : bool(ai_profile.get("requires_playwright", False)),
        "formatting_rules"     : fr,
        "ads_keywords_learned" : list(ai_profile.get("ads_keywords_learned") or []),
        "learned_chapters"     : list(range(1, n_chapters + 1)),
        "sample_urls"          : urls,
        "pipeline"             : pipeline_config.to_dict(),
        "profile_version"      : 2,
        "optimizer_score"      : pipeline_config.score,
    }

    if ai_profile.get("uncertain_fields"):
        profile["uncertain_fields"] = ai_profile["uncertain_fields"]  # type: ignore[typeddict-unknown-key]

    return profile  # type: ignore[return-value]


def _print_summary(tag: str, profile: SiteProfile) -> None:
    fr       = profile.get("formatting_rules") or {}
    pipeline = profile.get("pipeline") or {}
    score    = profile.get("optimizer_score", 0)
    print(
        f"\n  [{tag}] ✅ Profile saved!\n"
        f"     confidence        = {profile.get('confidence', 0):.2f}\n"
        f"     optimizer_score   = {score:.3f}\n"
        f"     content_selector  = {profile.get('content_selector')!r}\n"
        f"     title_selector    = {profile.get('title_selector')!r}\n"
        f"     next_selector     = {profile.get('next_selector')!r}\n"
        f"     remove            = {profile.get('remove_selectors', [])}\n"
        f"     nav_type          = {profile.get('nav_type')!r}\n"
        f"     tables/math       = {fr.get('tables', False)} / {fr.get('math_support', False)}\n"
        f"     pipeline.notes    = {pipeline.get('notes')!r}\n"
        f"     ads_kw            = {len(profile.get('ads_keywords_learned', []))}",
        flush=True,
    )
    if profile.get("uncertain_fields"):
        print(f"     ⚠ uncertain: {profile['uncertain_fields']}", flush=True)
    print(f"{'═'*62}\n", flush=True)