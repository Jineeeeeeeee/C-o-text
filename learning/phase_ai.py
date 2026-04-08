"""
learning/phase_ai.py — 10 AI calls orchestration.

Fix P1-10: import snippet thay vì _snippet.
  _snippet đã được rename thành snippet (public) trong ai/agents.py.
  _snippet = snippet alias vẫn còn nhưng best practice là dùng tên public.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from ai.client  import AIRateLimiter
from ai.agents  import (
    ai_dom_structure, ai_independent_check, ai_stability_check,
    ai_remove_audit, ai_title_deepdive, ai_special_content,
    ai_ads_deepscan, ai_nav_stress, ai_full_simulation,
    ai_master_synthesis, resolve_phase1_conflicts,
    snippet,   # Fix P1-10: public name, không còn _snippet
)

logger = logging.getLogger(__name__)


async def run_10_ai_calls_internal(
    chapters   : list[tuple[str, str]],
    domain     : str,
    ai_limiter : AIRateLimiter,
) -> dict | None:
    """
    Chạy 10 AI calls. Trả về dict selector profile hoặc None nếu fail nghiêm trọng.
    """
    urls  = [url  for url,  _ in chapters]
    htmls = [html for _, html in chapters]
    n     = len(chapters)

    all_results: dict[str, dict | None] = {}
    dangerous_selectors: set[str]        = set()

    # ── PHASE 1: Structure Discovery ──────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 1: Structure Discovery ━━", flush=True)

    print(f"  [Learn] 🤖 AI#1: DOM structure mapping (Ch.1+2)...", flush=True)
    ai1 = await ai_dom_structure(
        snippet(htmls[0], 10000), urls[0],
        snippet(htmls[1], 8000),  urls[1],
        ai_limiter,
    )
    all_results["ai1"] = ai1
    if ai1:
        print(
            f"     → content={ai1.get('content_selector')!r} "
            f"title={ai1.get('chapter_title_selector')!r} "
            f"next={ai1.get('next_selector')!r}",
            flush=True,
        )
    else:
        print(f"  [Learn] ⚠ AI#1 thất bại", flush=True)

    print(f"  [Learn] 🤖 AI#2: Independent cross-check (Ch.1+2)...", flush=True)
    ai2 = await ai_independent_check(
        snippet(htmls[0], 10000), urls[0],
        snippet(htmls[1], 8000),  urls[1],
        ai_limiter,
    )
    all_results["ai2"] = ai2
    if ai2:
        print(f"     → content={ai2.get('content_selector')!r} conf={ai2.get('confidence', 0):.2f}", flush=True)

    if not ai1 and not ai2:
        print(f"  [Learn] ✗ AI#1 và AI#2 đều thất bại — không thể học", flush=True)
        return None

    consensus, p1_conflicts = resolve_phase1_conflicts(ai1, ai2)
    if p1_conflicts:
        print(f"  [Learn] ⚠ {len(p1_conflicts)} conflicts Phase 1: {p1_conflicts}", flush=True)
    else:
        print(f"  [Learn] ✓ Phase 1: AI#1 và AI#2 đồng thuận", flush=True)

    if n >= 4:
        print(f"  [Learn] 🤖 AI#3: Selector stability (Ch.3+4)...", flush=True)
        ai3 = await ai_stability_check(
            snippet(htmls[2], 8000), urls[2],
            snippet(htmls[3], 8000), urls[3],
            consensus, ai_limiter,
        )
        all_results["ai3"] = ai3
        if ai3:
            score = ai3.get("stability_score", 0)
            print(f"     → stability_score={score:.2f}", flush=True)
            if ai3.get("content_fix"):
                consensus["content_selector"] = ai3["content_fix"]
            if ai3.get("title_fix"):
                consensus["chapter_title_selector"] = ai3["title_fix"]
            if ai3.get("next_fix"):
                consensus["next_selector"] = ai3["next_fix"]
            safe_rm   = set(ai3.get("remove_selectors_safe", []))
            dangerous = set(ai3.get("remove_selectors_dangerous", []))
            add_rm    = set(ai3.get("remove_add", []))
            dangerous_selectors.update(dangerous)
            current_rm = set(consensus.get("remove_selectors") or [])
            consensus["remove_selectors"] = list((current_rm & safe_rm) | add_rm - dangerous_selectors)
    else:
        all_results["ai3"] = None

    # ── PHASE 2: Conflict Resolution ──────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 2: Conflict Resolution ━━", flush=True)
    current_remove = list(consensus.get("remove_selectors") or [])

    if n >= 5 and current_remove:
        print(f"  [Learn] 🤖 AI#4: Remove selectors audit (Ch.5)...", flush=True)
        ai4 = await ai_remove_audit(
            snippet(htmls[4], 8000), urls[4],
            current_remove,
            consensus.get("content_selector"),
            consensus.get("chapter_title_selector"),
            ai_limiter,
        )
        all_results["ai4"] = ai4
        if ai4:
            newly_dangerous = set(ai4.get("dangerous_selectors") or [])
            dangerous_selectors.update(newly_dangerous)
            if newly_dangerous:
                print(f"     ⚠ {len(newly_dangerous)} dangerous selectors removed", flush=True)
            consensus["remove_selectors"] = [
                s for s in current_remove if s not in dangerous_selectors
            ]
    else:
        all_results["ai4"] = None

    if n >= 6:
        print(f"  [Learn] 🤖 AI#5: Title deep-dive (Ch.6)...", flush=True)
        ai5 = await ai_title_deepdive(
            snippet(htmls[5], 8000), urls[5],
            consensus.get("chapter_title_selector"),
            (ai1 or {}).get("author_selector") or (ai2 or {}).get("author_selector"),
            ai_limiter,
        )
        all_results["ai5"] = ai5
        if ai5:
            best = ai5.get("recommended_title_selector") or ai5.get("best_title_selector")
            if best and best != consensus.get("chapter_title_selector"):
                print(f"     → title selector refined: {best!r}", flush=True)
                consensus["chapter_title_selector"] = best
    else:
        all_results["ai5"] = None

    # ── PHASE 3: Content Intelligence ─────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 3: Content Intelligence ━━", flush=True)
    formatting_rules: dict = {}

    if n >= 7:
        print(f"  [Learn] 🤖 AI#6: Special content detection (Ch.7)...", flush=True)
        ai6 = await ai_special_content(snippet(htmls[6], 8000), urls[6], ai_limiter)
        all_results["ai6"] = ai6
        if ai6:
            formatting_rules.update({
                "tables"         : ai6.get("has_tables", False),
                "math_support"   : ai6.get("has_math", False),
                "math_format"    : ai6.get("math_format"),
                "special_symbols": ai6.get("special_symbols", []),
                "bold_italic"    : ai6.get("bold_italic", True),
                "hr_dividers"    : ai6.get("hr_dividers", True),
                "image_alt_text" : ai6.get("image_alt_text", False),
            })
            for key in ("system_box", "hidden_text", "author_note"):
                formatting_rules[key] = ai6.get(key, {"found": False, "selectors": []})
            if not formatting_rules["tables"]:
                if any("<table" in h.lower() for h in htmls):
                    formatting_rules["tables"] = True
        else:
            formatting_rules["tables"]       = any("<table" in h.lower() for h in htmls)
            formatting_rules["math_support"]  = False
    else:
        all_results["ai6"] = None

    ads_keywords: list[str] = []

    if n >= 8:
        print(f"  [Learn] 🤖 AI#7: Ads deep scan (Ch.8)...", flush=True)
        ai7 = await ai_ads_deepscan(snippet(htmls[7], 8000), urls[7], ai_limiter)
        all_results["ai7"] = ai7
        if ai7:
            ads_keywords = list(ai7.get("ads_keywords") or [])
            ads_sels = [s for s in (ai7.get("ads_selectors") or []) if s not in dangerous_selectors]
            if ads_sels:
                existing_rm = set(consensus.get("remove_selectors") or [])
                existing_rm.update(ads_sels)
                consensus["remove_selectors"] = list(existing_rm)
    else:
        all_results["ai7"] = None

    # ── PHASE 4: Stress Test ──────────────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 4: Stress Test ━━", flush=True)

    if n >= 9:
        print(f"  [Learn] 🤖 AI#8: Navigation stress test (Ch.9)...", flush=True)
        ai8 = await ai_nav_stress(
            snippet(htmls[8], 8000), urls[8],
            consensus.get("next_selector"),
            consensus.get("nav_type"),
            ai_limiter,
        )
        all_results["ai8"] = ai8
        if ai8:
            if not ai8.get("next_selector_works") and ai8.get("best_next_selector"):
                consensus["next_selector"] = ai8["best_next_selector"]
            if ai8.get("nav_type_confirmed"):
                consensus["nav_type"] = ai8["nav_type_confirmed"]
            if ai8.get("chapter_url_pattern_fix"):
                consensus["chapter_url_pattern"] = ai8["chapter_url_pattern_fix"]
    else:
        all_results["ai8"] = None

    profile_so_far = {
        "content_selector"      : consensus.get("content_selector"),
        "chapter_title_selector": consensus.get("chapter_title_selector"),
        "next_selector"         : consensus.get("next_selector"),
        "remove_selectors"      : consensus.get("remove_selectors", []),
        "nav_type"              : consensus.get("nav_type"),
    }

    if n >= 10:
        print(f"  [Learn] 🤖 AI#9: Full profile simulation (Ch.10)...", flush=True)
        ai9 = await ai_full_simulation(
            snippet(htmls[9], 8000), urls[9],
            profile_so_far, ai_limiter,
        )
        all_results["ai9"] = ai9
        if ai9:
            score = ai9.get("overall_score", 0)
            print(f"     → overall_score={score:.2f}", flush=True)
            if not ai9.get("removal_safe", True):
                print(f"     ⚠ Simulation: removal NOT safe — reverting", flush=True)
                consensus["remove_selectors"] = []
    else:
        all_results["ai9"] = None

    # ── PHASE 5: Master Synthesis ─────────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 5: Master Synthesis ━━", flush=True)
    print(f"  [Learn] 🤖 AI#10: Master profile synthesis...", flush=True)

    synthesis_summary = _build_synthesis_summary(
        all_results, consensus, dangerous_selectors, ads_keywords, formatting_rules, n,
    )
    ai10 = await ai_master_synthesis(synthesis_summary, domain, ai_limiter)
    all_results["ai10"] = ai10

    if ai10:
        print(
            f"     → confidence={ai10.get('confidence', 0):.2f} "
            f"uncertain={ai10.get('uncertain_fields', [])}",
            flush=True,
        )
        final_remove = [s for s in (ai10.get("remove_selectors") or []) if s not in dangerous_selectors]
        final_ads    = list({*ads_keywords, *(ai10.get("ads_keywords") or [])})
        final_title  = ai10.get("chapter_title_selector") or consensus.get("chapter_title_selector")
        return {
            "confidence"            : ai10.get("confidence", 0.7),
            "content_selector"      : ai10.get("content_selector") or consensus.get("content_selector"),
            "next_selector"         : ai10.get("next_selector")    or consensus.get("next_selector"),
            "title_selector"        : final_title,
            "chapter_title_selector": final_title,
            "remove_selectors"      : final_remove,
            "nav_type"              : ai10.get("nav_type")         or consensus.get("nav_type"),
            "chapter_url_pattern"   : ai10.get("chapter_url_pattern") or consensus.get("chapter_url_pattern"),
            "requires_playwright"   : bool(ai10.get("requires_playwright", False)),
            "formatting_rules"      : ai10.get("formatting_rules") or formatting_rules,
            "ads_keywords_learned"  : final_ads,
            "uncertain_fields"      : ai10.get("uncertain_fields", []),
        }
    else:
        print(f"  [Learn] ⚠ AI#10 thất bại — dùng consensus", flush=True)
        final_remove = [s for s in (consensus.get("remove_selectors") or []) if s not in dangerous_selectors]
        confidence   = _estimate_confidence(all_results, n)
        return {
            "confidence"            : confidence,
            "content_selector"      : consensus.get("content_selector"),
            "next_selector"         : consensus.get("next_selector"),
            "title_selector"        : consensus.get("chapter_title_selector"),
            "chapter_title_selector": consensus.get("chapter_title_selector"),
            "remove_selectors"      : final_remove,
            "nav_type"              : consensus.get("nav_type"),
            "chapter_url_pattern"   : consensus.get("chapter_url_pattern"),
            "requires_playwright"   : bool(consensus.get("requires_playwright", False)),
            "formatting_rules"      : formatting_rules,
            "ads_keywords_learned"  : ads_keywords,
            "uncertain_fields"      : [],
        }


def _build_synthesis_summary(results, consensus, dangerous_selectors, ads_keywords, formatting_rules, n_chapters):
    lines: list[str] = []
    lines.append(f"Chapters fetched: {n_chapters}")
    lines.append(f"\n--- PHASE 1 CONSENSUS ---")
    lines.append(f"content_selector      : {consensus.get('content_selector')!r}")
    lines.append(f"chapter_title_selector: {consensus.get('chapter_title_selector')!r}")
    lines.append(f"next_selector         : {consensus.get('next_selector')!r}")
    lines.append(f"nav_type              : {consensus.get('nav_type')!r}")
    lines.append(f"remove_selectors      : {consensus.get('remove_selectors', [])}")
    if dangerous_selectors:
        lines.append(f"\n--- ⚠ DANGEROUS SELECTORS ---")
        for s in sorted(dangerous_selectors):
            lines.append(f"  DANGEROUS: {s!r}")
    for label, key in [("AI#3", "ai3"), ("AI#4", "ai4"), ("AI#5", "ai5"),
                       ("AI#6", "ai6"), ("AI#7", "ai7"), ("AI#8", "ai8"), ("AI#9", "ai9")]:
        r = results.get(key) or {}
        if r:
            lines.append(f"\n--- {label} ---")
            for k, v in list(r.items())[:5]:
                lines.append(f"  {k}: {str(v)[:80]}")
    if formatting_rules:
        lines.append(f"\n--- FORMATTING ---")
        lines.append(f"tables: {formatting_rules.get('tables', False)}")
        lines.append(f"math:   {formatting_rules.get('math_support', False)}")
    if ads_keywords:
        lines.append(f"\n--- ADS ({len(ads_keywords)}) ---")
        lines.append(str(ads_keywords[:10]))
    return "\n".join(lines)


def _estimate_confidence(results: dict, n_chapters: int) -> float:
    scores: list[float] = []
    for key in ("ai3", "ai9", "ai2"):
        r = results.get(key) or {}
        score_key = (
            "stability_score" if key == "ai3"
            else "overall_score" if key == "ai9"
            else "confidence"
        )
        if r.get(score_key):
            scores.append(float(r[score_key]))
    base = 0.5 + 0.03 * min(n_chapters, 10)
    return round(sum(scores) / len(scores) if scores else base, 2)