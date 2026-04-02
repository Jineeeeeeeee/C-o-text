"""
ai/agents.py — Tất cả hàm gọi Gemini API.

Learning Phase agents (5 calls):
  ai_build_initial_profile()     — AI Call #1
  ai_validate_selectors()        — AI Call #2
  ai_analyze_special_content()   — AI Call #3
  ai_analyze_formatting()        — AI Call #4
  ai_final_crosscheck()          — AI Call #5

Utility agents:
  ai_find_first_chapter()        — Tìm Chapter 1 từ index page
  ai_classify_and_find()         — Emergency fallback khi không tìm được next URL
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from config import GEMINI_MODEL, RE_NEXT_BTN
from ai.client   import ai_client, AIRateLimiter
from ai.prompts  import Prompts


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


async def _call(prompt: str, limiter: AIRateLimiter, schema: dict[str, Any] | None = None) -> str | None:
    """
    Gọi Gemini với retry. Trả về text response hoặc None nếu thất bại.
    Nếu schema cung cấp → dùng structured output (application/json).
    """
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
            is_last = attempt >= _MAX_RETRIES - 1
            err_str = _fmt(e).lower()
            # Schema errors → fallback mà không dùng structured mode
            if schema and ("response_schema" in err_str or "mime_type" in err_str):
                try:
                    resp = await ai_client.aio.models.generate_content(
                        model=GEMINI_MODEL, contents=prompt,
                    )
                    return resp.text
                except Exception:
                    return None
            if _is_retriable(e) and not is_last:
                wait = _RETRY_BACKOFF[attempt]
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
    """Trả về HTML snippet đã rút gọn để gửi AI."""
    soup = BeautifulSoup(html, "html.parser")
    # Xóa script/style để tiết kiệm tokens
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


# ── JSON Schemas ──────────────────────────────────────────────────────────────

_S_INITIAL_PROFILE = {
    "type": "object",
    "properties": {
        "content_selector"   : {"type": "string",  "nullable": True},
        "next_selector"      : {"type": "string",  "nullable": True},
        "title_selector"     : {"type": "string",  "nullable": True},
        "remove_selectors"   : {"type": "array",   "items": {"type": "string"}},
        "nav_type"           : {"type": "string",  "nullable": True},
        "chapter_url_pattern": {"type": "string",  "nullable": True},
        "requires_playwright": {"type": "boolean"},
        "notes"              : {"type": "string",  "nullable": True},
    },
}

_S_VALIDATE = {
    "type": "object",
    "properties": {
        "content_valid": {"type": "boolean"},
        "content_fix"  : {"type": "string", "nullable": True},
        "next_valid"   : {"type": "boolean"},
        "next_fix"     : {"type": "string", "nullable": True},
        "title_valid"  : {"type": "boolean"},
        "title_fix"    : {"type": "string", "nullable": True},
        "remove_add"   : {"type": "array", "items": {"type": "string"}},
        "notes"        : {"type": "string", "nullable": True},
    },
    "required": ["content_valid", "next_valid", "title_valid"],
}

_S_SPECIAL = {
    "type": "object",
    "properties": {
        "has_tables"     : {"type": "boolean"},
        "table_evidence" : {"type": "string", "nullable": True},
        "has_math"       : {"type": "boolean"},
        "math_format"    : {"type": "string", "nullable": True},
        "math_evidence"  : {"type": "array", "items": {"type": "string"}},
        "special_symbols": {"type": "array", "items": {"type": "string"}},
        "notes"          : {"type": "string", "nullable": True},
    },
    "required": ["has_tables", "has_math"],
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

_S_FORMATTING = {
    "type": "object",
    "properties": {
        "system_box"    : _S_SPECIAL_ELEMENT,
        "hidden_text"   : _S_SPECIAL_ELEMENT,
        "author_note"   : _S_SPECIAL_ELEMENT,
        "bold_italic"   : {"type": "boolean"},
        "hr_dividers"   : {"type": "boolean"},
        "image_alt_text": {"type": "boolean"},
        "notes"         : {"type": "string", "nullable": True},
    },
    "required": ["bold_italic", "hr_dividers"],
}

_S_CROSSCHECK = {
    "type": "object",
    "properties": {
        "content_selector_final" : {"type": "string",  "nullable": True},
        "next_selector_final"    : {"type": "string",  "nullable": True},
        "title_selector_final"   : {"type": "string",  "nullable": True},
        "remove_selectors_final" : {"type": "array",   "items": {"type": "string"}},
        "ads_keywords"           : {"type": "array",   "items": {"type": "string"}},
        "confidence"             : {"type": "number"},
        "notes"                  : {"type": "string",  "nullable": True},
    },
    "required": ["confidence"],
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


# ── Learning Phase Agents ─────────────────────────────────────────────────────

async def ai_build_initial_profile(
    html: str, url: str, limiter: AIRateLimiter,
) -> dict | None:
    """AI Call #1 — Học selectors cơ bản từ Chapter 1."""
    prompt = Prompts.learning_1_initial_profile(_snippet(html), url)
    try:
        text = await _call(prompt, limiter, _S_INITIAL_PROFILE)
        result = _parse(text)
        if isinstance(result, dict):
            # Validate regex
            pat = result.get("chapter_url_pattern")
            if pat:
                try:
                    re.compile(pat)
                except re.error:
                    result["chapter_url_pattern"] = None
            # Normalize remove_selectors
            rm = result.get("remove_selectors")
            if not isinstance(rm, list):
                result["remove_selectors"] = []
            if not isinstance(result.get("requires_playwright"), bool):
                result["requires_playwright"] = False
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI #1] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_validate_selectors(
    html: str, url: str, current: dict, limiter: AIRateLimiter,
) -> dict | None:
    """AI Call #2 — Xác nhận selectors hoạt động trên Chapter 2."""
    prompt = Prompts.learning_2_validate(_snippet(html, 8000), url, current)
    try:
        text   = await _call(prompt, limiter, _S_VALIDATE)
        result = _parse(text)
        if isinstance(result, dict):
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI #2] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_analyze_special_content(
    html: str, url: str, limiter: AIRateLimiter,
) -> dict | None:
    """AI Call #3 — Phát hiện bảng, toán, ký hiệu đặc biệt từ Chapter 3."""
    prompt = Prompts.learning_3_special_content(_snippet(html, 8000), url)
    try:
        text   = await _call(prompt, limiter, _S_SPECIAL)
        result = _parse(text)
        if isinstance(result, dict):
            # Normalize
            if not isinstance(result.get("math_evidence"), list):
                result["math_evidence"] = []
            if not isinstance(result.get("special_symbols"), list):
                result["special_symbols"] = []
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI #3] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_analyze_formatting(
    html: str, url: str, limiter: AIRateLimiter,
) -> dict | None:
    """AI Call #4 — Phân tích system box, spoiler, author note từ Chapter 4."""
    prompt = Prompts.learning_4_formatting(_snippet(html, 8000), url)
    try:
        text   = await _call(prompt, limiter, _S_FORMATTING)
        result = _parse(text)
        if isinstance(result, dict):
            # Ensure boolean defaults
            result.setdefault("bold_italic",    True)
            result.setdefault("hr_dividers",    True)
            result.setdefault("image_alt_text", False)
            # Normalize special element rules
            for key in ("system_box", "hidden_text", "author_note"):
                rule = result.get(key)
                if not isinstance(rule, dict):
                    result[key] = {"found": False, "selectors": []}
                else:
                    rule.setdefault("found",      False)
                    rule.setdefault("selectors",  [])
                    if not isinstance(rule["selectors"], list):
                        rule["selectors"] = []
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI #4] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


async def ai_final_crosscheck(
    html: str, url: str, accumulated: dict, limiter: AIRateLimiter,
) -> dict | None:
    """AI Call #5 — Cross-check toàn bộ profile + confidence score."""
    prompt = Prompts.learning_5_final_crosscheck(_snippet(html, 8000), url, accumulated)
    try:
        text   = await _call(prompt, limiter, _S_CROSSCHECK)
        result = _parse(text)
        if isinstance(result, dict):
            # Clamp confidence
            try:
                result["confidence"] = max(0.0, min(1.0, float(result.get("confidence", 0.7))))
            except (TypeError, ValueError):
                result["confidence"] = 0.7
            if not isinstance(result.get("ads_keywords"), list):
                result["ads_keywords"] = []
            if not isinstance(result.get("remove_selectors_final"), list):
                result["remove_selectors_final"] = []
            # Normalize keywords: lowercase, strip
            result["ads_keywords"] = [
                kw.lower().strip() for kw in result["ads_keywords"]
                if isinstance(kw, str) and kw.strip()
            ]
            return result
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  [AI #5] ⚠ Thất bại: {_fmt(e)}", flush=True)
    return None


# ── Utility Agents ────────────────────────────────────────────────────────────

async def ai_find_first_chapter(
    html: str, base_url: str, limiter: AIRateLimiter,
) -> str | None:
    """Tìm URL Chapter 1 từ trang Index."""
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

    return links[0]  # Fallback: link đầu tiên trong list


async def ai_classify_and_find(
    html: str, base_url: str, limiter: AIRateLimiter,
) -> dict | None:
    """Emergency fallback — phân loại trang và tìm next URL."""
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