"""
learning/phase.py — Learning Phase orchestrator (v3).

Thay đổi so với v2:
  ARCH-1: Sau khi 10 AI calls học selectors, gọi thêm run_optimizer()
          để tìm pipeline config tốt nhất từ candidates heuristic.
  ARCH-2: Profile mới lưu cả selectors cũ (backward compat) VÀ pipeline config mới.
  ARCH-3: Phase này giờ là thin orchestrator — logic nặng đã chuyển vào
          learning/optimizer.py và pipeline/*.py.

Fix H2: Đọc CAO_FAST_LEARNING env var.
  Khi --fast-learning được truyền qua CLI, main.py set CAO_FAST_LEARNING=1.
  Phase này giờ kiểm tra flag đó và skip run_optimizer() — thay vào đó
  dùng default pipeline + merge AI selectors thủ công.
  Tiết kiệm ~30% thời gian learning (không cần eval 8 candidates × 5 chapters).

Flow mới:
  1. Fetch 10 chapters (giữ nguyên)
  2. 10 AI calls học selectors (giữ nguyên — từ phase cũ)
  3. [CONDITIONAL] run_optimizer() nếu KHÔNG có --fast-learning
     Hoặc: dùng default pipeline + merge AI selectors nếu CÓ --fast-learning
  4. Merge AI selectors vào winner pipeline (optimizer tự xử lý)
  5. Save profile với cả "pipeline" key lẫn selector fields cũ

Tại sao giữ 10 AI calls?
  AI calls học selectors CỤ THỂ (content_selector, title_selector, v.v.)
  Optimizer học CHIẾN LƯỢC (thứ tự ưu tiên, fallback chain).
  Hai việc bổ sung cho nhau — không thay thế nhau.
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
from ai.agents import (
    ai_dom_structure, ai_independent_check, ai_stability_check,
    ai_remove_audit, ai_title_deepdive, ai_special_content,
    ai_ads_deepscan, ai_nav_stress, ai_full_simulation,
    ai_master_synthesis, ai_classify_and_find, ai_find_first_chapter,
    resolve_phase1_conflicts,
)

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
        (profile, sample_raw_titles, fetched_chapters)
        fetched_chapters = [(url, html)] — tái dùng cho scraping
        None nếu thất bại nghiêm trọng
    """
    from utils.string_helpers import domain_tag as _dtag
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    # Fix H2: đọc flag từ env — được set bởi main.py khi --fast-learning
    fast_learning = os.getenv("CAO_FAST_LEARNING") == "1"
    if fast_learning:
        print(f"  [{tag}] ⚡ Fast-learning mode: optimizer sẽ bị skip", flush=True)

    print(f"\n{'═'*62}", flush=True)
    print(f"  🎓 Deep Learning: {domain}", flush=True)
    print(f"  📚 Fetching {LEARNING_CHAPTERS} chapters...", flush=True)
    print(f"{'═'*62}", flush=True)

    # ── 1. Fetch chapters ─────────────────────────────────────────────────────
    chapters, curl_htmls = await _fetch_chapters(
        start_url, pool, pw_pool, pm, ai_limiter, domain,
    )

    if len(chapters) < 4:
        print(
            f"  [{tag}] ✗ Chỉ fetch được {len(chapters)}/{LEARNING_CHAPTERS} chapters"
            f" — không đủ để học.",
            flush=True,
        )
        return None

    n = len(chapters)
    print(f"  [{tag}] ✓ Fetched {n}/{LEARNING_CHAPTERS} chapters\n", flush=True)

    # ── 2. 10 AI calls (học selectors) ───────────────────────────────────────
    ai_profile = await _run_10_ai_calls(chapters, domain, ai_limiter)
    if ai_profile is None:
        print(f"  [{tag}] ⚠ 10 AI calls thất bại — dùng empty profile cho optimizer", flush=True)
        ai_profile = {}

    # ── 3. Optimizer hoặc fast-learning path ─────────────────────────────────
    if fast_learning:
        # Skip optimizer: dùng default pipeline + inject AI selectors thủ công.
        # Tiết kiệm ~8 candidates × 5 chapters × pipeline overhead.
        print(f"  [{tag}] ⚡ Fast-learning: skip optimizer, dùng default pipeline", flush=True)
        from pipeline.base import PipelineConfig
        from learning.optimizer import _merge_ai_selectors
        pipeline_config = PipelineConfig.default_for_domain(domain)
        _merge_ai_selectors(pipeline_config, ai_profile)
        pipeline_config.notes = "fast_learning_default"
        # score = confidence từ AI (optimizer không chạy nên không có eval score)
        pipeline_config.score = float(ai_profile.get("confidence", 0.5))
    else:
        print(f"\n  [{tag}] 🔧 Pipeline Optimizer...", flush=True)
        try:
            pipeline_config = await run_optimizer(
                domain           = domain,
                chapters         = chapters,
                existing_profile = ai_profile,
                pool             = pool,
                pw_pool          = pw_pool,
                ai_limiter       = ai_limiter,
                curl_htmls       = curl_htmls,
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

    # ── 5. Print summary ──────────────────────────────────────────────────────
    _print_summary(tag, profile)

    # ── 6. Extract raw titles cho Naming Phase ────────────────────────────────
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
) -> tuple[list[tuple[str, str]], list[str]]:
    """
    Fetch LEARNING_CHAPTERS chapters.

    Returns:
        (chapters, curl_htmls)
        chapters   = [(url, playwright_html)]
        curl_htmls = [curl_html] — dùng để detect JS-heavy trong optimizer
    """
    from utils.string_helpers import domain_tag as _dtag
    tag = _dtag(domain)

    chapters  : list[tuple[str, str]] = []
    curl_htmls: list[str]             = []
    current_url = start_url

    # Index page → tìm Chapter 1
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

        print(
            f"  [{tag}] Fetch Ch.{i+1:>2}/{LEARNING_CHAPTERS}"
            f" → {current_url[:60]}",
            flush=True,
        )

        try:
            if i == 0:
                # Ch.1: Playwright để đảm bảo full render
                status, html = await pw_pool.fetch(current_url)
                # Fetch curl song song để detect JS-heavy
                try:
                    _, curl_html = await pool.fetch(current_url)
                    curl_htmls.append(curl_html)
                except Exception:
                    curl_htmls.append("")
            else:
                status, html = await fetch_page(current_url, pool, pw_pool)
                curl_htmls.append("")  # placeholder
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
                print(
                    f"  [{tag}] ⚠ Heuristic nav thất bại Ch.{i+1} → AI fallback...",
                    flush=True,
                )
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

    return chapters, curl_htmls


# ── 10 AI calls (giữ nguyên logic từ phase.py cũ) ────────────────────────────

async def _run_10_ai_calls(
    chapters   : list[tuple[str, str]],
    domain     : str,
    ai_limiter : AIRateLimiter,
) -> dict | None:
    """
    Chạy 10 AI calls để học selectors.
    Trả về dict với content_selector, next_selector, title_selector, v.v.
    Trả về None nếu thất bại nghiêm trọng (AI#1 và AI#2 đều fail).
    """
    from learning.phase_ai import run_10_ai_calls_internal  # type: ignore[import]
    return await run_10_ai_calls_internal(chapters, domain, ai_limiter)


# ── Profile builder ───────────────────────────────────────────────────────────

def _build_final_profile(
    domain         : str,
    ai_profile     : dict,
    pipeline_config,
    n_chapters     : int,
    chapters       : list[tuple[str, str]],
) -> SiteProfile:
    """
    Build final SiteProfile kết hợp AI selectors + pipeline config.
    Giữ nguyên tất cả fields cũ để backward compatible.
    """
    urls = [url for url, _ in chapters]

    # Lấy formatting_rules từ ai_profile
    fr = ai_profile.get("formatting_rules") or {}

    profile: SiteProfile = {
        # ── Fields cũ (backward compat) ──────────────────────────────────────
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

        # ── Fields mới (pipeline architecture) ───────────────────────────────
        "pipeline"             : pipeline_config.to_dict(),
        "profile_version"      : 2,
        "optimizer_score"      : pipeline_config.score,
    }

    # Copy uncertain_fields nếu có
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
        f"     tables/math       = {fr.get('tables', False)}"
        f" / {fr.get('math_support', False)}\n"
        f"     pipeline.notes    = {pipeline.get('notes')!r}\n"
        f"     ads_kw            = {len(profile.get('ads_keywords_learned', []))}",
        flush=True,
    )
    if profile.get("uncertain_fields"):
        print(f"     ⚠ uncertain: {profile['uncertain_fields']}", flush=True)

    print(f"{'═'*62}\n", flush=True)