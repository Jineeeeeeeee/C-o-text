"""
learning/phase.py — Thorough Learning Mode: 5 AI calls để build complete profile.

Flow:
  1. Fetch Chapter 1 (Playwright full render)
  2. AI #1 interleaved ngay sau Ch.1 → get next_selector để navigate
  3. Fetch Chapters 2–5 dùng profile tạm thời (có next_selector từ AI #1)
  4. AI #2 → validate selectors với Chapter 2
  5. AI #3 → analyze special content (tables, math) với Chapter 3
  6. AI #4 → analyze formatting (system box, spoiler, author note) với Chapter 4
  7. AI #5 → final cross-check + confidence score với Chapter 5
  8. Merge tất cả → SiteProfile hoàn chỉnh
  9. Save profile → Phase 3 scrape lại từ Ch.1

TỔNG: 5 AI calls, không gọi trùng.

Tại sao Playwright cho Ch.1:
  - Ch.1 là nền tảng của toàn bộ profile
  - Một số site render content qua JS (data-src, lazy load)
  - Chi phí nhỏ (1 lần) nhưng đảm bảo HTML đầy đủ nhất
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import LEARNING_CHAPTERS, get_delay
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
)

logger = logging.getLogger(__name__)


async def run_learning_phase(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
) -> SiteProfile | None:
    """
    Chạy Thorough Learning Mode.

    Returns:
        SiteProfile hoàn chỉnh nếu thành công
        None nếu thất bại (ít nhất 2 chapters phải fetch được và AI #1 phải pass)
    """
    domain = urlparse(start_url).netloc.lower()
    print(f"\n🎓 [Learn] Bắt đầu Thorough Learning Mode cho {domain}", flush=True)

    # ── Bước 1: Fetch chapters + AI #1 interleaved ────────────────────────────
    # _fetch_chapters trả về (chapters, ai1_result).
    # AI #1 chạy ngay sau Ch.1 (không phải cuối) để có next_selector cho Ch.2+.
    # _run_ai_calls nhận ai1_result → không cần gọi AI #1 lần thứ hai.
    chapters, ai1_result = await _fetch_chapters(
        start_url, pool, pw_pool, pm, ai_limiter, domain
    )

    if len(chapters) < 2:
        print(
            f"  [Learn] ✗ Chỉ fetch được {len(chapters)}/{LEARNING_CHAPTERS} chapters. "
            f"Cần ít nhất 2 để học.",
            flush=True,
        )
        return None

    if ai1_result is None:
        print(f"  [Learn] ✗ AI #1 thất bại — không thể build profile.", flush=True)
        return None

    print(f"  [Learn] ✓ Fetch xong {len(chapters)}/{LEARNING_CHAPTERS} chapters", flush=True)

    # ── Bước 2–5: AI calls #2–5 ──────────────────────────────────────────────
    profile = await _run_ai_calls(chapters, domain, ai_limiter, ai1_result)
    if profile is None:
        return None

    # ── Bước 6: Save profile ──────────────────────────────────────────────────
    await pm.save_profile(domain, profile)
    fr = profile.get("formatting_rules") or {}
    print(
        f"\n  [Learn] ✅ Profile saved!\n"
        f"     confidence   = {profile.get('confidence', 0):.2f}\n"
        f"     content_sel  = {profile.get('content_selector')!r}\n"
        f"     next_sel     = {profile.get('next_selector')!r}\n"
        f"     title_sel    = {profile.get('title_selector')!r}\n"
        f"     remove_sels  = {profile.get('remove_selectors', [])}\n"
        f"     nav_type     = {profile.get('nav_type')!r}\n"
        f"     tables       = {fr.get('tables', False)}\n"
        f"     math         = {fr.get('math_support', False)}"
        + (f" [{fr.get('math_format')}]" if fr.get('math_support') else "") + "\n"
        f"     system_box   = {bool(fr.get('system_box', {}).get('found'))}\n"
        f"     hidden_text  = {bool(fr.get('hidden_text', {}).get('found'))}\n"
        f"     author_note  = {bool(fr.get('author_note', {}).get('found'))}\n"
        f"     ads_keywords = {len(profile.get('ads_keywords_learned', []))}",
        flush=True,
    )

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
    """
    Fetch LEARNING_CHAPTERS chapters và chạy AI #1 interleaved sau Ch.1.

    Returns:
        (chapters, ai1_result)
        chapters   : list[(url, html)] theo thứ tự
        ai1_result : dict từ ai_build_initial_profile, hoặc None nếu thất bại
    """
    chapters:   list[tuple[str, str]] = []
    ai1_result: dict | None           = None

    current_url = start_url
    # Giữ profile tạm (có thể đã có từ lần chạy trước, hoặc empty dict)
    temp_profile: SiteProfile = pm.get(domain)  # type: ignore[assignment]

    for i in range(LEARNING_CHAPTERS):
        if not current_url:
            break

        print(
            f"  [Learn] Fetch Ch.{i+1}/{LEARNING_CHAPTERS} → {current_url[:65]}",
            flush=True,
        )

        # ── Fetch ─────────────────────────────────────────────────────────────
        try:
            if i == 0:
                # Ch.1: luôn Playwright để đảm bảo full JS render
                status, html = await pw_pool.fetch(current_url)
            else:
                status, html = await fetch_page(current_url, pool, pw_pool)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [Learn] ⚠ Fetch Ch.{i+1} thất bại: {e}", flush=True)
            break

        if is_junk_page(html, status):
            print(f"  [Learn] ⚠ Ch.{i+1} junk page (status={status})", flush=True)
            break

        chapters.append((current_url, html))

        # ── AI #1: chạy ngay sau Ch.1 ─────────────────────────────────────────
        # Mục đích kép:
        #   a) Lấy next_selector để navigate sang Ch.2 ngay
        #   b) Lưu result để _run_ai_calls dùng lại — KHÔNG gọi lại
        if i == 0:
            print(f"  [Learn] 🤖 AI #1: Build initial profile...", flush=True)
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
                print(f"  [Learn] ⚠ AI #1 thất bại — navigation fallback mode", flush=True)
                # Vẫn tiếp tục: sẽ dùng heuristic để navigate

        # ── Tìm next URL ──────────────────────────────────────────────────────
        if i < LEARNING_CHAPTERS - 1:
            soup     = BeautifulSoup(html, "html.parser")
            next_url = find_next_url(soup, current_url, temp_profile)

            if not next_url:
                print(
                    f"  [Learn] ⚠ Heuristic navigation thất bại Ch.{i+1} → thử AI...",
                    flush=True,
                )
                try:
                    ai_nav = await ai_classify_and_find(html, current_url, ai_limiter)
                    if ai_nav:
                        next_url = ai_nav.get("next_url")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[Learn] AI nav thất bại: %s", e)

            if not next_url:
                print(f"  [Learn] ⚠ Không tìm được next URL sau Ch.{i+1}", flush=True)
                break

            current_url = next_url

            # Delay lịch sự giữa các chapters
            await asyncio.sleep(get_delay(current_url))

    return chapters, ai1_result


def _apply_ai1_to_profile(base: SiteProfile, ai1: dict) -> SiteProfile:
    """
    Merge kết quả AI #1 vào base profile tạm thời.
    Chỉ dùng để navigation trong Learning Phase — không lưu xuống disk.
    """
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
    """
    Orchestrate AI calls #2–5 và build SiteProfile hoàn chỉnh.

    ai1_result đã được gọi trong _fetch_chapters (interleaved) — nhận qua tham số,
    không gọi lại để tránh lãng phí quota.

    Tổng AI calls: 1 (AI#1 từ fetch) + tối đa 4 (AI#2–5) = tối đa 5.
    """
    urls  = [url  for url,  _ in chapters]
    htmls = [html for _, html in chapters]

    # State tích lũy — bắt đầu từ kết quả AI #1
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

    # ── AI #2: Validate selectors với Ch.2 ───────────────────────────────────
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

    # ── AI #3: Special content (tables, math, symbols) từ Ch.3 ───────────────
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

    # ── AI #4: Formatting analysis (boxes, spoilers, notes) từ Ch.4 ──────────
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

    # ── AI #5: Final cross-check + confidence ────────────────────────────────
    confidence:   float      = _default_confidence(len(htmls))
    ads_keywords: list[str]  = []

    if len(htmls) >= 5:
        print(f"  [Learn] 🤖 AI #5: Final cross-check + confidence score...", flush=True)
        ai5 = await ai_final_crosscheck(htmls[4], urls[4], acc, ai_limiter)
        if ai5:
            # Áp dụng final selector tweaks
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

            # Final remove_selectors: dùng danh sách tổng hợp từ AI #5
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

    # ── Build final SiteProfile ───────────────────────────────────────────────
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _default_confidence(n_chapters: int) -> float:
    """
    Confidence mặc định khi AI #5 không chạy được.
    Tỉ lệ với số chapters đã probe.
    """
    # 1 ch → 0.5, 2 ch → 0.6, 3 ch → 0.7, 4 ch → 0.75
    return min(0.5 + 0.1 * (n_chapters - 1), 0.75)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()