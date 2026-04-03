# learning/phase.py
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import LEARNING_CHAPTERS, get_delay, RE_CHAP_URL
from utils.types import SiteProfile
from utils.string_helpers import is_junk_page
from core.fetch        import fetch_page
from core.session_pool import DomainSessionPool, PlaywrightPool
from core.navigator    import find_next_url
from learning.profile_manager import ProfileManager
from ai.client  import AIRateLimiter
from ai.agents  import (
    ai_build_initial_profile,
    ai_validate_selectors,
    ai_analyze_special_content,
    ai_analyze_formatting,
    ai_final_crosscheck,
    ai_classify_and_find,
    ai_find_first_chapter,
)

logger = logging.getLogger(__name__)


async def run_learning_phase(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
) -> SiteProfile | None:
    from core.scraper import _dtag  # import local để tránh circular
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    print(f"\n{'═'*62}", flush=True)
    print(f"  🎓 Learning: {domain}", flush=True)
    print(f"{'═'*62}", flush=True)

    chapters, ai1_result = await _fetch_chapters(
        start_url, pool, pw_pool, pm, ai_limiter, domain
    )

    if len(chapters) < 2:
        print(
            f"  [{tag}] ✗ Chỉ fetch được {len(chapters)}/{LEARNING_CHAPTERS} chapters.",
            flush=True,
        )
        return None

    if ai1_result is None:
        print(f"  [{tag}] ✗ AI #1 thất bại.", flush=True)
        return None

    print(f"  [{tag}] ✓ {len(chapters)}/{LEARNING_CHAPTERS} chapters fetched", flush=True)

    profile = await _run_ai_calls(chapters, domain, ai_limiter, ai1_result)
    if profile is None:
        return None

    await pm.save_profile(domain, profile)
    fr = profile.get("formatting_rules") or {}
    print(
        f"\n  [{tag}] ✅ Profile saved!\n"
        f"     confidence  = {profile.get('confidence', 0):.2f}\n"
        f"     content     = {profile.get('content_selector')!r}\n"
        f"     next        = {profile.get('next_selector')!r}\n"
        f"     title       = {profile.get('title_selector')!r}\n"
        f"     remove      = {profile.get('remove_selectors', [])}\n"
        f"     nav_type    = {profile.get('nav_type')!r}\n"
        f"     tables/math = {fr.get('tables', False)} / {fr.get('math_support', False)}\n"
        f"     system_box  = {bool(fr.get('system_box', {}).get('found'))}\n"
        f"     ads_kw      = {len(profile.get('ads_keywords_learned', []))}",
        flush=True,
    )
    print(f"{'═'*62}\n", flush=True)

    return profile


# ── Chapter fetching ──────────────────────────────────────────────────────────

async def _fetch_chapters(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
    domain     : str,
) -> tuple[list[tuple[str, str]], dict | None]:
    chapters:   list[tuple[str, str]] = []
    ai1_result: dict | None           = None

    # ── FIX: Nếu start_url là trang Index → tìm Chapter 1 thật sự trước ─────
    current_url = start_url
    if not RE_CHAP_URL.search(start_url):
        print(f"  [{tag}] 📋 start_url có vẻ là trang Index → tìm Chapter 1...", flush=True)
        try:
            status, index_html = await pw_pool.fetch(start_url)
            if not is_junk_page(index_html, status):
                first_url = await ai_find_first_chapter(index_html, start_url, ai_limiter)
                if first_url and first_url != start_url:
                    print(f"  [{tag}] ✅ Chapter 1: {first_url[:70]}", flush=True)
                    current_url = first_url
                else:
                    print(f"  [{tag}] ⚠ Không tìm được Chapter 1, dùng start_url", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [{tag}] ⚠ Index detection thất bại: {e}", flush=True)

    temp_profile: SiteProfile = pm.get(domain)  # type: ignore[assignment]

    for i in range(LEARNING_CHAPTERS):
        if not current_url:
            break

        print(
            f"  [{tag}] Fetch Ch.{i+1}/{LEARNING_CHAPTERS} → {current_url[:65]}",
            flush=True,
        )

        try:
            if i == 0:
                status, html = await pw_pool.fetch(current_url)
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

        if i == 0:
            print(f"  [{tag}] 🤖 AI #1: Build initial profile...", flush=True)
            ai1_result = await ai_build_initial_profile(html, current_url, ai_limiter)
            if ai1_result:
                temp_profile = _apply_ai1_to_profile(temp_profile, ai1_result)
                print(
                    f"     → content={ai1_result.get('content_selector')!r} "
                    f"next={ai1_result.get('next_selector')!r} "
                    f"nav={ai1_result.get('nav_type')!r}",
                    flush=True,
                )
            else:
                print(f"  [{tag}] ⚠ AI #1 thất bại — navigation fallback mode", flush=True)

        if i < LEARNING_CHAPTERS - 1:
            soup     = BeautifulSoup(html, "html.parser")
            next_url = find_next_url(soup, current_url, temp_profile)

            if not next_url:
                print(
                    f"  [{tag}] ⚠ Heuristic navigation thất bại Ch.{i+1} → thử AI...",
                    flush=True,
                )
                try:
                    ai_nav = await ai_classify_and_find(html, current_url, ai_limiter)
                    if ai_nav:
                        next_url = ai_nav.get("next_url")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[%s] AI nav thất bại: %s", e)

            if not next_url:
                print(f"  [{tag}] ⚠ Không tìm được next URL sau Ch.{i+1}", flush=True)
                break

            current_url = next_url
            await asyncio.sleep(get_delay(current_url))

    return chapters, ai1_result


def _apply_ai1_to_profile(base: SiteProfile, ai1: dict) -> SiteProfile:
    result = dict(base)
    for field in (
        "content_selector", "next_selector", "title_selector",
        "nav_type", "chapter_url_pattern", "requires_playwright",
    ):
        val = ai1.get(field)
        if val is not None:
            result[field] = val  # type: ignore[literal-required]
    rm = ai1.get("remove_selectors")
    if isinstance(rm, list):
        result["remove_selectors"] = rm  # type: ignore[typeddict-unknown-key]
    return result  # type: ignore[return-value]


# ── AI calls orchestration ────────────────────────────────────────────────────

async def _run_ai_calls(
    chapters   : list[tuple[str, str]],
    domain     : str,
    ai_limiter : AIRateLimiter,
    ai1_result : dict,
) -> SiteProfile | None:
    urls  = [url  for url,  _ in chapters]
    htmls = [html for _, html in chapters]

    acc: dict = {
        "content_selector"   : ai1_result.get("content_selector"),
        "next_selector"      : ai1_result.get("next_selector"),
        "title_selector"     : ai1_result.get("title_selector"),
        "remove_selectors"   : list(ai1_result.get("remove_selectors") or []),
        "nav_type"           : ai1_result.get("nav_type"),
        "chapter_url_pattern": ai1_result.get("chapter_url_pattern"),
        "requires_playwright": ai1_result.get("requires_playwright", False),
        "formatting_rules"   : {},
    }

    if len(htmls) >= 2:
        print(f"  [Learn] 🤖 AI #2: Validate selectors với Ch.2...", flush=True)
        ai2 = await ai_validate_selectors(htmls[1], urls[1], acc, ai_limiter)
        if ai2:
            changed: list[str] = []
            if not ai2.get("content_valid") and ai2.get("content_fix"):
                acc["content_selector"] = ai2["content_fix"]
                changed.append(f"content={ai2['content_fix']!r}")
            if not ai2.get("next_valid") and ai2.get("next_fix"):
                acc["next_selector"] = ai2["next_fix"]
                changed.append(f"next={ai2['next_fix']!r}")
            if not ai2.get("title_valid") and ai2.get("title_fix"):
                acc["title_selector"] = ai2["title_fix"]
                changed.append(f"title={ai2['title_fix']!r}")
            for sel in (ai2.get("remove_add") or []):
                if sel and sel not in acc["remove_selectors"]:
                    acc["remove_selectors"].append(sel)
            if changed:
                print(f"     → Fixed: {', '.join(changed)}", flush=True)
            else:
                print(f"     → All selectors valid ✓", flush=True)
        else:
            print(f"  [Learn] ⚠ AI #2 thất bại — giữ selectors từ AI #1", flush=True)

    if len(htmls) >= 3:
        print(f"  [Learn] 🤖 AI #3: Detect tables / math / symbols từ Ch.3...", flush=True)
        ai3 = await ai_analyze_special_content(htmls[2], urls[2], ai_limiter)
        if ai3:
            fr = acc["formatting_rules"]
            fr["tables"]          = ai3.get("has_tables", False)
            fr["math_support"]    = ai3.get("has_math", False)
            fr["math_format"]     = ai3.get("math_format")
            fr["special_symbols"] = ai3.get("special_symbols", [])
            if ai3.get("has_tables"):
                print(f"     → Tables: {ai3.get('table_evidence', 'yes')}", flush=True)
            if ai3.get("has_math"):
                print(f"     → Math [{ai3.get('math_format')}]: {ai3.get('math_evidence', [])[:2]}", flush=True)
            if ai3.get("special_symbols"):
                print(f"     → Symbols: {ai3.get('special_symbols', [])[:8]}", flush=True)
        else:
            print(f"  [Learn] ⚠ AI #3 thất bại — skip special content detection", flush=True)

    if len(htmls) >= 4:
        print(f"  [Learn] 🤖 AI #4: Analyze formatting elements từ Ch.4...", flush=True)
        ai4 = await ai_analyze_formatting(htmls[3], urls[3], ai_limiter)
        if ai4:
            fr = acc["formatting_rules"]
            for key in ("system_box", "hidden_text", "author_note"):
                rule = ai4.get(key)
                if isinstance(rule, dict):
                    fr[key] = rule
                    if rule.get("found") and rule.get("selectors"):
                        print(f"     → {key}: {rule['selectors']}", flush=True)
            fr["bold_italic"]    = ai4.get("bold_italic",    True)
            fr["hr_dividers"]    = ai4.get("hr_dividers",    True)
            fr["image_alt_text"] = ai4.get("image_alt_text", False)
        else:
            print(f"  [Learn] ⚠ AI #4 thất bại — skip formatting analysis", flush=True)

    confidence:   float      = _default_confidence(len(htmls))
    ads_keywords: list[str]  = []

    if len(htmls) >= 5:
        print(f"  [Learn] 🤖 AI #5: Final cross-check + confidence score...", flush=True)
        ai5 = await ai_final_crosscheck(htmls[4], urls[4], acc, ai_limiter)
        if ai5:
            for field, key in (
                ("content_selector", "content_selector_final"),
                ("next_selector",    "next_selector_final"),
                ("title_selector",   "title_selector_final"),
            ):
                val = ai5.get(key)
                if val:
                    if acc.get(field) != val:
                        print(f"     → {field} refined: {val!r}", flush=True)
                    acc[field] = val

            final_rm = ai5.get("remove_selectors_final")
            if isinstance(final_rm, list) and final_rm:
                acc["remove_selectors"] = final_rm

            confidence   = ai5.get("confidence", confidence)
            ads_keywords = ai5.get("ads_keywords", [])
            print(f"     → confidence = {confidence:.2f}", flush=True)
        else:
            print(f"  [Learn] ⚠ AI #5 thất bại — dùng confidence mặc định {confidence:.2f}", flush=True)
    else:
        print(
            f"  [Learn] ℹ️  {len(htmls)} chapters → confidence mặc định {confidence:.2f}",
            flush=True,
        )

    profile: SiteProfile = {
        "domain"             : domain,
        "last_learned"       : _now_iso(),
        "confidence"         : confidence,
        "content_selector"   : acc.get("content_selector"),
        "next_selector"      : acc.get("next_selector"),
        "title_selector"     : acc.get("title_selector"),
        "remove_selectors"   : acc.get("remove_selectors", []),
        "nav_type"           : acc.get("nav_type"),
        "chapter_url_pattern": acc.get("chapter_url_pattern"),
        "requires_playwright": acc.get("requires_playwright", False),
        "formatting_rules"   : acc.get("formatting_rules", {}),
        "ads_keywords_learned": ads_keywords,
        "learned_chapters"   : list(range(1, len(htmls) + 1)),
        "sample_urls"        : urls,
    }

    return profile


def _default_confidence(n_chapters: int) -> float:
    return min(0.5 + 0.1 * (n_chapters - 1), 0.75)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()