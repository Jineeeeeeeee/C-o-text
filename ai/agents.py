"""
ai/agents.py — Tất cả hàm gọi Gemini API.

Learning Phase agents (10 calls):
  ai_dom_structure()         — AI#1: DOM mapping từ Ch.1+2
  ai_independent_check()     — AI#2: Cross-check độc lập từ Ch.1+2
  ai_stability_check()       — AI#3: Selector stability trên Ch.3+4
  ai_remove_audit()          — AI#4: Remove selectors conflict audit Ch.5
  ai_title_deepdive()        — AI#5: Title extraction deep-dive Ch.6
  ai_special_content()       — AI#6: Special content detection Ch.7
  ai_ads_deepscan()          — AI#7: Ads & watermark deep scan Ch.8
  ai_nav_stress()            — AI#8: Navigation stress test Ch.9
  ai_full_simulation()       — AI#9: Full profile simulation Ch.10
  ai_master_synthesis()      — AI#10: Master synthesis từ 9 calls

Utility agents:
  ai_extract_naming_rules()  — Story name + chapter naming pattern
  ai_find_first_chapter()    — Tìm Chapter 1 từ index page
  ai_classify_and_find()     — Emergency fallback next URL
  ai_verify_ads()            — Xác nhận ads/watermark
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from config import GEMINI_MODEL, RE_NEXT_BTN
from ai.client  import ai_client, AIRateLimiter
from ai.prompts import Prompts


# ── Retry infrastructure ──────────────────────────────────────────────────────

_MAX_RETRIES   = 3
_RETRY_BACKOFF = [30, 60]


def _is_retriable(e: Exception) -> bool:
    code = getattr(e, "status_code", None) or getattr(e, "code", None)
    if code in (429, 503):
        return True
    msg = (str(e) or repr(e)).lower()
    return any(kw in msg for kw in ("429", "503", "quota", "resource_exhausted", "unavailable"))


def _fmt(e: Exception) -> str:
    return (str(e) or repr(e)).strip()


async def _call(
    prompt: str,
    limiter: AIRateLimiter,
    schema: dict[str, Any] | None = None,
) -> str | None:
    """Gọi Gemini với retry. Trả về text response hoặc None nếu thất bại."""
    await limiter.acquire()
    for attempt in range(_MAX_RETRIES):
        try:
            if schema:
                from google.genai import types as T
                config = T.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=schema,
                )
                resp = await ai_client.aio.models.generate_content(
                    model=GEMINI_MODEL, contents=prompt, config=config,
                )
            else:
                resp = await ai_client.aio.models.generate_content(
                    model=GEMINI_MODEL, contents=prompt,
                )
            return resp.text
        except asyncio.CancelledError:
            raise
        except Exception as e:
            is_last  = attempt >= _MAX_RETRIES - 1
            err_str  = _fmt(e).lower()
            if schema and ("response_schema" in err_str or "mime_type" in err_str):
                try:
                    resp = await ai_client.aio.models.generate_content(
                        model=GEMINI_MODEL, contents=prompt,
                    )
                    return resp.text
                except Exception:
                    return None
            if _is_retriable(e) and not is_last:
                wait = _RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)]
                print(f"  [AI] ⚠ Rate limit/503 (lần {attempt+1}), thử lại sau {wait}s", flush=True)
                await asyncio.sleep(wait)
            else:
                raise
    return None


def _parse(text: str | None) -> dict | list | None:
    """Parse JSON từ AI response — strip markdown fences nếu có."""
    if not text:
        return None
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
    if m:
        text = m.group(1)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _snippet(html: str, max_len: int = 10000) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup.find_all(["script", "style", "noscript"]):
        t.decompose()
    return str(soup)[:max_len]


def _nav_hints(html: str, base_url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    hints = [
        f"{a.get_text(strip=True)!r} → {urljoin(base_url, a['href'])}"
        for a in soup.find_all("a", href=True)
        if RE_NEXT_BTN.search(a.get_text(strip=True))
    ]
    return "\n".join(hints[:10]) or "(không có)"


_RE_CHAP_LINK = re.compile(
    r"/(chapter|chuong|chap|/c/|/ch/|episode|ep)[_\-]?\d+"
    r"|/s/\d+/\d+",
    re.IGNORECASE,
)
_RE_TOC_PATH = re.compile(
    r"/(chapters|chapter-list|table-of-contents|toc|contents)[/?#]?$",
    re.IGNORECASE,
)


def _chapter_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _RE_TOC_PATH.search(href):
            continue
        if not _RE_CHAP_LINK.search(href):
            continue
        full = urljoin(base_url, href)
        if full not in seen:
            seen.add(full)
            links.append(full)
    return links


# ── Conflict resolution helper ────────────────────────────────────────────────

def _resolve_selector_conflict(
    result1: dict | None,
    result2: dict | None,
    field: str,
) -> tuple[str | None, bool]:
    """
    So sánh 2 AI results cho cùng 1 field.
    Returns: (winning_value, is_conflict)
    """
    v1 = (result1 or {}).get(field)
    v2 = (result2 or {}).get(field)

    if v1 == v2:
        return v1, False  # Agree

    if not v1 and v2:
        return v2, False  # Only one has value
    if v1 and not v2:
        return v1, False

    # Both have different values → conflict
    # Tiebreak: prefer result with higher confidence
    c1 = float((result1 or {}).get("confidence", 0.5))
    c2 = float((result2 or {}).get("confidence", 0.5))
    if c1 >= c2:
        return v1, True
    return v2, True


def resolve_phase1_conflicts(
    ai1: dict | None,
    ai2: dict | None,
) -> tuple[dict, list[str]]:
    """
    Resolve conflicts giữa AI#1 và AI#2.
    Returns: (consensus_dict, list_of_conflicted_fields)
    """
    conflicts: list[str] = []
    consensus: dict = {}

    fields = [
        "content_selector",
        "chapter_title_selector",
        "next_selector",
        "nav_type",
        "chapter_url_pattern",
    ]

    for field in fields:
        val, is_conflict = _resolve_selector_conflict(ai1, ai2, field)
        consensus[field] = val
        if is_conflict:
            conflicts.append(field)
            print(
                f"  [Learn] ⚠ Conflict on {field!r}: "
                f"AI#1={str((ai1 or {}).get(field))[:40]!r} vs "
                f"AI#2={str((ai2 or {}).get(field))[:40]!r}",
                flush=True,
            )

    # Remove selectors: lấy INTERSECTION (chỉ giữ selectors cả 2 AI agree)
    rm1 = set((ai1 or {}).get("remove_selectors") or [])
    rm2 = set((ai2 or {}).get("remove_selectors") or [])
    if rm1 and rm2:
        consensus["remove_selectors"] = list(rm1 & rm2)  # intersection = conservative
        only_in_1 = rm1 - rm2
        only_in_2 = rm2 - rm1
        if only_in_1 or only_in_2:
            print(
                f"  [Learn] ℹ Remove selectors: "
                f"{len(consensus['remove_selectors'])} agreed, "
                f"{len(only_in_1)} only-AI1, {len(only_in_2)} only-AI2 → dùng intersection",
                flush=True,
            )
    elif rm1:
        consensus["remove_selectors"] = list(rm1)
    elif rm2:
        consensus["remove_selectors"] = list(rm2)
    else:
        consensus["remove_selectors"] = []

    # requires_playwright: OR (nếu ai nói cần thì cần)
    consensus["requires_playwright"] = bool(
        (ai1 or {}).get("requires_playwright", False) or
        (ai2 or {}).get("requires_playwright", False)
    )

    return consensus, conflicts


# ── JSON Schemas ──────────────────────────────────────────────────────────────

_S_DOM_STRUCTURE = {
    "type": "object",
    "properties": {
        "chapter_title_selector"         : {"type": "string",  "nullable": True},
        "story_title_selector"           : {"type": "string",  "nullable": True},
        "author_selector"                : {"type": "string",  "nullable": True},
        "content_selector"               : {"type": "string",  "nullable": True},
        "next_selector"                  : {"type": "string",  "nullable": True},
        "remove_selectors"               : {"type": "array",   "items": {"type": "string"}},
        "nav_type"                       : {"type": "string",  "nullable": True},
        "chapter_url_pattern"            : {"type": "string",  "nullable": True},
        "requires_playwright"            : {"type": "boolean"},
        "title_is_inside_remove_candidate": {"type": "boolean"},
        "title_container"                : {"type": "string",  "nullable": True},
        "notes"                          : {"type": "string",  "nullable": True},
    },
}

_S_INDEPENDENT_CHECK = {
    "type": "object",
    "properties": {
        "chapter_title_selector": {"type": "string",  "nullable": True},
        "content_selector"      : {"type": "string",  "nullable": True},
        "next_selector"         : {"type": "string",  "nullable": True},
        "remove_selectors"      : {"type": "array",   "items": {"type": "string"}},
        "nav_type"              : {"type": "string",  "nullable": True},
        "chapter_url_pattern"   : {"type": "string",  "nullable": True},
        "author_selector"       : {"type": "string",  "nullable": True},
        "confidence"            : {"type": "number"},
        "uncertain_fields"      : {"type": "array",   "items": {"type": "string"}},
        "notes"                 : {"type": "string",  "nullable": True},
    },
    "required": ["confidence"],
}

_S_STABILITY = {
    "type": "object",
    "properties": {
        "content_valid_ch3"          : {"type": "boolean"},
        "content_valid_ch4"          : {"type": "boolean"},
        "content_fix"                : {"type": "string",  "nullable": True},
        "title_valid_ch3"            : {"type": "boolean"},
        "title_valid_ch4"            : {"type": "boolean"},
        "title_fix"                  : {"type": "string",  "nullable": True},
        "next_valid_ch3"             : {"type": "boolean"},
        "next_valid_ch4"             : {"type": "boolean"},
        "next_fix"                   : {"type": "string",  "nullable": True},
        "remove_selectors_safe"      : {"type": "array",   "items": {"type": "string"}},
        "remove_selectors_dangerous" : {"type": "array",   "items": {"type": "string"}},
        "remove_add"                 : {"type": "array",   "items": {"type": "string"}},
        "stability_score"            : {"type": "number"},
        "notes"                      : {"type": "string",  "nullable": True},
    },
    "required": ["stability_score"],
}

_S_REMOVE_AUDIT = {
    "type": "object",
    "properties": {
        "audit_results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "selector"                  : {"type": "string"},
                    "is_ancestor_of_content"    : {"type": "boolean"},
                    "is_ancestor_of_title"      : {"type": "boolean"},
                    "contains_title_or_content" : {"type": "boolean"},
                    "verdict"                   : {"type": "string"},
                    "reason"                    : {"type": "string", "nullable": True},
                },
            },
        },
        "safe_selectors"         : {"type": "array", "items": {"type": "string"}},
        "dangerous_selectors"    : {"type": "array", "items": {"type": "string"}},
        "suggested_replacements" : {"type": "object"},
        "notes"                  : {"type": "string", "nullable": True},
    },
    "required": ["safe_selectors", "dangerous_selectors"],
}

_S_TITLE_DEEPDIVE = {
    "type": "object",
    "properties": {
        "best_title_selector"      : {"type": "string",  "nullable": True},
        "author_name_detected"     : {"type": "string",  "nullable": True},
        "author_contamination_risk": {"type": "boolean"},
        "title_cleanup_needed"     : {"type": "boolean"},
        "title_cleanup_note"       : {"type": "string",  "nullable": True},
        "recommended_title_selector": {"type": "string", "nullable": True},
        "notes"                    : {"type": "string",  "nullable": True},
    },
}

_S_SPECIAL_ELEMENT = {
    "type": "object",
    "properties": {
        "found"     : {"type": "boolean"},
        "selectors" : {"type": "array", "items": {"type": "string"}},
        "convert_to": {"type": "string"},
        "prefix"    : {"type": "string"},
    },
    "required": ["found"],
}

_S_SPECIAL_CONTENT = {
    "type": "object",
    "properties": {
        "has_tables"    : {"type": "boolean"},
        "table_evidence": {"type": "string",  "nullable": True},
        "has_math"      : {"type": "boolean"},
        "math_format"   : {"type": "string",  "nullable": True},
        "math_evidence" : {"type": "array",   "items": {"type": "string"}},
        "system_box"    : _S_SPECIAL_ELEMENT,
        "hidden_text"   : _S_SPECIAL_ELEMENT,
        "author_note"   : _S_SPECIAL_ELEMENT,
        "bold_italic"   : {"type": "boolean"},
        "hr_dividers"   : {"type": "boolean"},
        "image_alt_text": {"type": "boolean"},
        "special_symbols": {"type": "array",  "items": {"type": "string"}},
        "notes"         : {"type": "string",  "nullable": True},
    },
    "required": ["has_tables", "has_math"],
}

_S_ADS_DEEPSCAN = {
    "type": "object",
    "properties": {
        "ads_keywords"        : {"type": "array", "items": {"type": "string"}},
        "ads_selectors"       : {"type": "array", "items": {"type": "string"}},
        "top_edge_pattern"    : {"type": "string", "nullable": True},
        "bottom_edge_pattern" : {"type": "string", "nullable": True},
        "notes"               : {"type": "string", "nullable": True},
    },
    "required": ["ads_keywords"],
}

_S_NAV_STRESS = {
    "type": "object",
    "properties": {
        "next_selector_works"      : {"type": "boolean"},
        "next_url_found"           : {"type": "string",  "nullable": True},
        "best_next_selector"       : {"type": "string",  "nullable": True},
        "nav_type_confirmed"       : {"type": "string",  "nullable": True},
        "chapter_url_pattern_valid": {"type": "boolean"},
        "chapter_url_pattern_fix"  : {"type": "string",  "nullable": True},
        "fallback_methods"         : {"type": "array",   "items": {"type": "string"}},
        "notes"                    : {"type": "string",  "nullable": True},
    },
    "required": ["next_selector_works"],
}

_S_SIMULATION = {
    "type": "object",
    "properties": {
        "content_extracted" : {"type": "string",  "nullable": True},
        "content_char_count": {"type": "integer"},
        "content_quality"   : {"type": "string"},
        "title_extracted"   : {"type": "string",  "nullable": True},
        "title_quality"     : {"type": "string"},
        "next_url_found"    : {"type": "string",  "nullable": True},
        "nav_quality"       : {"type": "string"},
        "removed_elements"  : {"type": "array",   "items": {"type": "string"}},
        "removal_safe"      : {"type": "boolean"},
        "overall_score"     : {"type": "number"},
        "issues_found"      : {"type": "array",   "items": {"type": "string"}},
        "field_scores"      : {"type": "object"},
        "notes"             : {"type": "string",  "nullable": True},
    },
    "required": ["overall_score"],
}

_S_MASTER = {
    "type": "object",
    "properties": {
        "content_selector"       : {"type": "string",  "nullable": True},
        "next_selector"          : {"type": "string",  "nullable": True},
        "chapter_title_selector" : {"type": "string",  "nullable": True},
        "remove_selectors"       : {"type": "array",   "items": {"type": "string"}},
        "nav_type"               : {"type": "string",  "nullable": True},
        "chapter_url_pattern"    : {"type": "string",  "nullable": True},
        "requires_playwright"    : {"type": "boolean"},
        "formatting_rules"       : {"type": "object"},
        "ads_keywords"           : {"type": "array",   "items": {"type": "string"}},
        "confidence"             : {"type": "number"},
        "uncertain_fields"       : {"type": "array",   "items": {"type": "string"}},
        "conflict_summary"       : {"type": "string",  "nullable": True},
        "notes"                  : {"type": "string",  "nullable": True},
    },
    "required": ["confidence"],
}

_S_NAMING_RULES = {
    "type": "object",
    "properties": {
        "story_name"           : {"type": "string"},
        "story_prefix_to_strip": {"type": "string"},
        "chapter_keyword"      : {"type": "string"},
        "has_chapter_subtitle" : {"type": "boolean"},
        "notes"                : {"type": "string", "nullable": True},
    },
    "required": ["story_name", "chapter_keyword", "has_chapter_subtitle"],
}

_S_FIRST_CHAPTER = {
    "type": "object",
    "properties": {
        "first_chapter_url": {"type": "string", "nullable": True},
    },
}

_S_CLASSIFY = {
    "type": "object",
    "properties": {
        "page_type"        : {"type": "string", "enum": ["chapter", "index", "other"]},
        "next_url"         : {"type": "string", "nullable": True},
        "first_chapter_url": {"type": "string", "nullable": True},
    },
    "required": ["page_type"],
}

_S_VERIFY_ADS = {
    "type": "object",
    "properties": {
        "confirmed_ads" : {"type": "array", "items": {"type": "string"}},
        "false_positives": {"type": "array", "items": {"type": "string"}},
        "notes"         : {"type": "string", "nullable": True},
    },
    "required": ["confirmed_ads"],
}


# ══════════════════════════════════════════════════════════════════════════════
# LEARNING PHASE AGENTS
# ══════════════════════════════════════════════════════════════════════════════

async def ai_dom_structure(
    html1: str, url1: str,
    html2: str, url2: str,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#1 — Initial DOM structure mapping từ Ch.1+2."""
    prompt = Prompts.learning_1_dom_structure(
        _snippet(html1, 10000), url1,
        _snippet(html2, 8000),  url2,
    )
    try:
        text   = await _call(prompt, limiter, _S_DOM_STRUCTURE)
        result = _parse(text)
        if isinstance(result, dict):
            _sanitize_remove_selectors(result)
            _validate_regex_field(result, "chapter_url_pattern")
            result.setdefault("requires_playwright", False)
            result.setdefault("title_is_inside_remove_candidate", False)
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#1] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_independent_check(
    html1: str, url1: str,
    html2: str, url2: str,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#2 — Independent cross-check (cùng data Ch.1+2, hoàn toàn độc lập)."""
    prompt = Prompts.learning_2_independent_check(
        _snippet(html1, 10000), url1,
        _snippet(html2, 8000),  url2,
    )
    try:
        text   = await _call(prompt, limiter, _S_INDEPENDENT_CHECK)
        result = _parse(text)
        if isinstance(result, dict):
            _sanitize_remove_selectors(result)
            _validate_regex_field(result, "chapter_url_pattern")
            try:
                result["confidence"] = max(0.0, min(1.0, float(result.get("confidence", 0.7))))
            except (TypeError, ValueError):
                result["confidence"] = 0.7
            result.setdefault("uncertain_fields", [])
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#2] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_stability_check(
    html3: str, url3: str,
    html4: str, url4: str,
    consensus: dict,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#3 — Selector stability validation trên Ch.3+4."""
    prompt = Prompts.learning_3_stability_check(
        _snippet(html3, 8000), url3,
        _snippet(html4, 8000), url4,
        consensus,
    )
    try:
        text   = await _call(prompt, limiter, _S_STABILITY)
        result = _parse(text)
        if isinstance(result, dict):
            result.setdefault("remove_selectors_safe",      [])
            result.setdefault("remove_selectors_dangerous", [])
            result.setdefault("remove_add",                 [])
            try:
                result["stability_score"] = max(0.0, min(1.0, float(result.get("stability_score", 0.8))))
            except (TypeError, ValueError):
                result["stability_score"] = 0.8
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#3] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_remove_audit(
    html5: str, url5: str,
    remove_selectors: list[str],
    content_selector: str | None,
    title_selector: str | None,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#4 — Remove selectors conflict audit trên Ch.5."""
    prompt = Prompts.learning_4_remove_audit(
        _snippet(html5, 8000), url5,
        remove_selectors, content_selector, title_selector,
    )
    try:
        text   = await _call(prompt, limiter, _S_REMOVE_AUDIT)
        result = _parse(text)
        if isinstance(result, dict):
            result.setdefault("safe_selectors",      [])
            result.setdefault("dangerous_selectors", [])
            result.setdefault("audit_results",       [])
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#4] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_title_deepdive(
    html6: str, url6: str,
    title_selector: str | None,
    author_selector: str | None,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#5 — Title extraction deep-dive + author contamination check trên Ch.6."""
    prompt = Prompts.learning_5_title_deepdive(
        _snippet(html6, 8000), url6,
        title_selector, author_selector,
    )
    try:
        text   = await _call(prompt, limiter, _S_TITLE_DEEPDIVE)
        result = _parse(text)
        if isinstance(result, dict):
            result.setdefault("author_contamination_risk", False)
            result.setdefault("title_cleanup_needed",      False)
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#5] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_special_content(
    html7: str, url7: str,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#6 — Special content detection (tables/math/system box) trên Ch.7."""
    prompt = Prompts.learning_6_special_content(_snippet(html7, 8000), url7)
    try:
        text   = await _call(prompt, limiter, _S_SPECIAL_CONTENT)
        result = _parse(text)
        if isinstance(result, dict):
            result.setdefault("math_evidence",   [])
            result.setdefault("special_symbols", [])
            result.setdefault("bold_italic",     True)
            result.setdefault("hr_dividers",     True)
            result.setdefault("image_alt_text",  False)
            for key in ("system_box", "hidden_text", "author_note"):
                rule = result.get(key)
                if not isinstance(rule, dict):
                    result[key] = {"found": False, "selectors": []}
                else:
                    rule.setdefault("found",     False)
                    rule.setdefault("selectors", [])
                    if not isinstance(rule["selectors"], list):
                        rule["selectors"] = []
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#6] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_ads_deepscan(
    html8: str, url8: str,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#7 — Ads & watermark deep scan trên Ch.8."""
    prompt = Prompts.learning_7_ads_deepscan(_snippet(html8, 8000), url8)
    try:
        text   = await _call(prompt, limiter, _S_ADS_DEEPSCAN)
        result = _parse(text)
        if isinstance(result, dict):
            result.setdefault("ads_keywords",     [])
            result.setdefault("ads_selectors",    [])
            result["ads_keywords"] = [
                kw.lower().strip()
                for kw in result["ads_keywords"]
                if isinstance(kw, str) and kw.strip()
            ]
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#7] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_nav_stress(
    html9: str, url9: str,
    next_selector: str | None,
    nav_type: str | None,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#8 — Navigation stress test trên Ch.9."""
    prompt = Prompts.learning_8_nav_stress(
        _snippet(html9, 8000), url9,
        next_selector, nav_type,
    )
    try:
        text   = await _call(prompt, limiter, _S_NAV_STRESS)
        result = _parse(text)
        if isinstance(result, dict):
            result.setdefault("next_selector_works", False)
            result.setdefault("fallback_methods",    [])
            _validate_regex_field(result, "chapter_url_pattern_fix")
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#8] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_full_simulation(
    html10: str, url10: str,
    profile_so_far: dict,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#9 — Full profile simulation + quality scoring trên Ch.10."""
    prompt = Prompts.learning_9_full_simulation(
        _snippet(html10, 8000), url10,
        profile_so_far,
    )
    try:
        text   = await _call(prompt, limiter, _S_SIMULATION)
        result = _parse(text)
        if isinstance(result, dict):
            try:
                result["overall_score"] = max(0.0, min(1.0, float(result.get("overall_score", 0.7))))
            except (TypeError, ValueError):
                result["overall_score"] = 0.7
            result.setdefault("issues_found",    [])
            result.setdefault("removed_elements",[])
            result.setdefault("removal_safe",    True)
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#9] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_master_synthesis(
    synthesis_summary: str,
    domain: str,
    limiter: AIRateLimiter,
) -> dict | None:
    """AI#10 — Master synthesis: tổng hợp profile cuối cùng từ 9 calls."""
    prompt = Prompts.learning_10_master_synthesis(synthesis_summary, domain)
    try:
        text   = await _call(prompt, limiter, _S_MASTER)
        result = _parse(text)
        if isinstance(result, dict):
            try:
                result["confidence"] = max(0.0, min(1.0, float(result.get("confidence", 0.7))))
            except (TypeError, ValueError):
                result["confidence"] = 0.7
            _sanitize_remove_selectors(result)
            _validate_regex_field(result, "chapter_url_pattern")
            result.setdefault("uncertain_fields", [])
            result.setdefault("ads_keywords",     [])
            result["ads_keywords"] = [
                kw.lower().strip()
                for kw in result["ads_keywords"]
                if isinstance(kw, str) and kw.strip()
            ]
            # Sanitize formatting_rules
            fr = result.get("formatting_rules")
            if not isinstance(fr, dict):
                result["formatting_rules"] = {}
            _sanitize_formatting_rules(result["formatting_rules"])
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI#10] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY AGENTS (giữ nguyên)
# ══════════════════════════════════════════════════════════════════════════════

async def ai_extract_naming_rules(
    raw_titles: list[str],
    base_url  : str,
    limiter   : AIRateLimiter,
) -> dict | None:
    if not raw_titles:
        return None
    prompt = Prompts.naming_rules(raw_titles, base_url)
    try:
        text   = await _call(prompt, limiter, _S_NAMING_RULES)
        result = _parse(text)
        if isinstance(result, dict) and result.get("story_name", "").strip():
            result["story_name"]            = result["story_name"].strip()
            result["story_prefix_to_strip"] = (result.get("story_prefix_to_strip") or "").strip()
            result["chapter_keyword"]       = (result.get("chapter_keyword") or "Chapter").strip()
            result["has_chapter_subtitle"]  = bool(result.get("has_chapter_subtitle", False))
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI naming] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_find_first_chapter(
    html: str, base_url: str, limiter: AIRateLimiter,
) -> str | None:
    links = await asyncio.to_thread(_chapter_links, html, base_url)
    if not links:
        return None
    if len(links) == 1:
        return links[0]
    candidates = "\n".join(links[:15])
    prompt = Prompts.find_first_chapter(candidates, base_url)
    try:
        text   = await _call(prompt, limiter, _S_FIRST_CHAPTER)
        result = _parse(text)
        if isinstance(result, dict) and result.get("first_chapter_url"):
            return result["first_chapter_url"]
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI find_first] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return links[0]


async def ai_classify_and_find(
    html: str, base_url: str, limiter: AIRateLimiter,
) -> dict | None:
    hints   = await asyncio.to_thread(_nav_hints, html, base_url)
    snippet = await asyncio.to_thread(_snippet, html, 5000)
    prompt  = Prompts.classify_and_find(hints, snippet, base_url)
    try:
        text   = await _call(prompt, limiter, _S_CLASSIFY)
        result = _parse(text)
        if isinstance(result, dict):
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI classify] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_verify_ads(
    candidates: list[str],
    domain    : str,
    limiter   : AIRateLimiter,
) -> list[str]:
    if not candidates:
        return []
    prompt = Prompts.verify_ads(candidates, domain)
    try:
        text   = await _call(prompt, limiter, _S_VERIFY_ADS)
        result = _parse(text)
        if isinstance(result, dict):
            confirmed = result.get("confirmed_ads") or []
            fp        = result.get("false_positives") or []
            if fp:
                print(
                    f"  [Ads] ℹ️  {len(fp)} false positive: "
                    + ", ".join(repr(x[:40]) for x in fp[:3]),
                    flush=True,
                )
            return [
                line for line in confirmed
                if isinstance(line, str) and line.strip()
            ]
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI verify_ads] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return []

# Schema cho ai_extract_content
_S_EXTRACT_CONTENT = {
    "type": "object",
    "properties": {
        "content"   : {"type": "string"},
        "confidence": {"type": "number"},
        "notes"     : {"type": "string", "nullable": True},
    },
    "required": ["content", "confidence"],
}
 
 
async def ai_extract_content(
    html     : str,
    url      : str,
    limiter  : AIRateLimiter,
) -> str | None:
    """
    Extract chapter content từ HTML bằng AI — last resort cho AIExtractBlock.

    Dùng khi tất cả heuristic blocks (selector, json_ld, density, fallback_list)
    đã thất bại. Gọi Gemini để identify main content.

    Fix M3: bỏ block fallback thứ hai vốn ignore confidence hoàn toàn.
    Cả hai điều kiện phải thoả — content đủ dài VÀ AI đủ tự tin.
    AI tự báo confidence=0.0 nghĩa là nó không tìm thấy content thực sự
    (có thể là nav text, error page, gibberish) — không nên trả về.

    Thresholds:
        MIN_CHARS      = 150  — tránh trả về snippet quá ngắn
        MIN_CONFIDENCE = 0.3  — AI phải tối thiểu "hơi tự tin"
                                (0.3 là thấp nhưng đủ để loại conf=0.0/0.1)

    Returns:
        str — chapter content đã extract, hoặc None nếu thất bại.
    """
    _MIN_CHARS      = 150
    _MIN_CONFIDENCE = 0.3

    prompt = Prompts.extract_content(_snippet(html, 8000), url)
    try:
        text   = await _call(prompt, limiter, _S_EXTRACT_CONTENT)
        result = _parse(text)
        if isinstance(result, dict):
            content = (result.get("content") or "").strip()
            conf    = float(result.get("confidence", 0.0))

            if len(content) >= _MIN_CHARS and conf >= _MIN_CONFIDENCE:
                return content

            # Log lý do từ chối để dễ debug
            if content and len(content) < _MIN_CHARS:
                print(
                    f"  [AI extract] ⚠ Từ chối: content quá ngắn"
                    f" ({len(content)}c < {_MIN_CHARS}c)",
                    flush=True,
                )
            elif content:
                print(
                    f"  [AI extract] ⚠ Từ chối: confidence quá thấp"
                    f" ({conf:.2f} < {_MIN_CONFIDENCE})",
                    flush=True,
                )

    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI extract] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None

# ── Sanitization helpers ──────────────────────────────────────────────────────

def _sanitize_remove_selectors(result: dict) -> None:
    rm = result.get("remove_selectors")
    if not isinstance(rm, list):
        result["remove_selectors"] = []
    else:
        result["remove_selectors"] = [
            s for s in rm if isinstance(s, str) and s.strip()
        ]


def _validate_regex_field(result: dict, field: str) -> None:
    pat = result.get(field)
    if pat:
        try:
            re.compile(pat)
        except re.error:
            result[field] = None


def _sanitize_formatting_rules(fr: dict) -> None:
    fr.setdefault("tables",         False)
    fr.setdefault("math_support",   False)
    fr.setdefault("math_format",    None)
    fr.setdefault("special_symbols",[])
    fr.setdefault("bold_italic",    True)
    fr.setdefault("hr_dividers",    True)
    fr.setdefault("image_alt_text", False)
    for key in ("system_box", "hidden_text", "author_note"):
        rule = fr.get(key)
        if not isinstance(rule, dict):
            fr[key] = {"found": False, "selectors": []}
        else:
            rule.setdefault("found",     False)
            rule.setdefault("selectors", [])
            if not isinstance(rule["selectors"], list):
                rule["selectors"] = []