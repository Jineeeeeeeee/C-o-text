"""
core/extractor.py — Trích xuất content và title sử dụng site profile.

extract_chapter():
  1. Áp dụng remove_selectors từ profile
  2. Tìm content element bằng content_selector
  3. Format bằng MarkdownFormatter (profile rules)
  4. Trích tiêu đề bằng title_selector hoặc fallback

Khi profile chưa có (learning phase): dùng fallback selectors + plain text.
"""
from __future__ import annotations

import re
from collections import Counter
from urllib.parse import urlparse, unquote

from bs4 import BeautifulSoup, Tag

from config import FALLBACK_CONTENT_SELECTORS
from core.formatter import MarkdownFormatter, extract_plain_text
from utils.types import SiteProfile
from utils.string_helpers import normalize_title, strip_site_suffix


_MIN_CONTENT_LEN = 200  # Chars tối thiểu để coi là content hợp lệ


def extract_chapter(
    soup: BeautifulSoup,
    url: str,
    profile: SiteProfile,
) -> tuple[str, str, str | None]:
    """
    Trích content và title từ trang chương.

    Returns:
        (content_markdown, title, winning_selector)
        content_markdown = "" nếu không tìm được
        title = "Unknown Title" nếu không tìm được
        winning_selector = CSS selector đã dùng, hoặc None
    """
    formatter = MarkdownFormatter(profile.get("formatting_rules"))
    content, selector = _extract_content(soup, profile, formatter)
    title = _extract_title(soup, url, profile)
    return content, title, selector


def _extract_content(
    soup: BeautifulSoup,
    profile: SiteProfile,
    formatter: MarkdownFormatter,
) -> tuple[str, str | None]:
    """Thử content_selector → fallback selectors. Trả (text, selector_used)."""

    def _try(sel: str) -> str | None:
        try:
            el = soup.select_one(sel)
            if not el:
                return None
            text = formatter.format(el)
            if len(text.strip()) >= _MIN_CONTENT_LEN:
                return text
        except Exception:
            pass
        return None

    # 1. Profile selector (AI-learned, highest priority)
    cs = profile.get("content_selector")
    if cs:
        text = _try(cs)
        if text:
            return text, cs

    # 2. Fallback list
    for sel in FALLBACK_CONTENT_SELECTORS:
        text = _try(sel)
        if text:
            return text, sel

    # 3. body fallback (last resort)
    body = soup.find("body")
    if body and isinstance(body, Tag):
        text = extract_plain_text(body)
        if len(text.strip()) >= _MIN_CONTENT_LEN:
            return text, "body"

    return "", None


def _extract_title(
    soup: BeautifulSoup,
    url: str,
    profile: SiteProfile,
) -> str:
    """
    Trích tiêu đề chương bằng đa-nguồn + majority vote.
    Thứ tự ưu tiên: profile selector > h1 > h2 > <title> > og:title > URL slug
    """
    candidates: list[str] = []

    # 1. Profile title selector
    ts = profile.get("title_selector")
    if ts:
        try:
            el = soup.select_one(ts)
            if el:
                text = normalize_title(el.get_text(strip=True))
                if len(text) >= 3:
                    candidates.extend([text, text])  # weight x2
        except Exception:
            pass

    # 2. h1 / h2
    for tag in ("h1", "h2"):
        el = soup.find(tag)
        if el:
            text = normalize_title(el.get_text(strip=True))
            if len(text) >= 3:
                candidates.append(text)

    # 3. <title> tag — strip site suffix (VD: "Chapter 5 | RoyalRoad" → "Chapter 5")
    title_tag = soup.find("title")
    if title_tag:
        raw = title_tag.get_text(strip=True)
        # Chỉ strip suffix nếu có dấu | hoặc — (site separator rõ ràng)
        if re.search(r"[\|–—]", raw):
            raw = strip_site_suffix(raw)
        raw = normalize_title(raw)
        if len(raw) >= 3:
            candidates.append(raw)

    # 4. og:title — tương tự, strip site suffix
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        raw = og["content"].strip()
        if re.search(r"[\|–—]", raw):
            raw = strip_site_suffix(raw)
        raw = normalize_title(raw)
        if len(raw) >= 3:
            candidates.append(raw)

    # 5. URL slug fallback
    slug = _title_from_url(url)
    if slug:
        candidates.append(slug)

    if not candidates:
        return "Unknown Title"

    # Majority vote
    counts = Counter(t.lower() for t in candidates)
    top2   = counts.most_common(2)
    if len(top2) == 1 or top2[0][1] > top2[1][1]:
        winner_lower = top2[0][0]
        for c in candidates:
            if c.lower() == winner_lower:
                return c
    # Tie: return longest (thường cụ thể hơn)
    return max(candidates, key=len)


def _title_from_url(url: str) -> str | None:
    """Trích title candidate từ URL path."""
    try:
        path  = urlparse(url).path.rstrip("/")
        parts = [p for p in path.split("/") if p]
        if not parts:
            return None

        # fanfiction.net: /s/{id}/{num}/
        if len(parts) >= 3 and parts[0] == "s" and parts[1].isdigit():
            if parts[2].isdigit():
                return f"Chapter {parts[2]}"

        slug = unquote(parts[-1])
        if slug.isdigit():
            return f"Chapter {slug}"

        m = re.match(r"chapter[-_](\d+)([-_].+)?", slug, re.IGNORECASE)
        if m:
            num   = m.group(1)
            extra = m.group(2) or ""
            extra = re.sub(r"^[-_]", "", extra).replace("-", " ").replace("_", " ").strip()
            return f"Chapter {num}" + (f" - {extra.title()}" if extra else "")

        words = re.split(r"[-_]", slug)
        title = " ".join(w.capitalize() for w in words if w)
        return title or None
    except Exception:
        return None