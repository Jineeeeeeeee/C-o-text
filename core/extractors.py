# core/extractors.py
"""
core/extractors.py — Trích xuất tiêu đề chương và tên truyện.

API thay đổi:
  TitleExtractor.extract(soup, url, ai_limiter?)  — nhận BeautifulSoup thay vì str
  extract_story_title(soup, url)                  — không đổi (đã nhận soup)

FIX-D: Wire `ai_validate_title` khi majority vote hòa (thay vì max(len)).
       ai_limiter là optional — nếu None thì fallback về hành vi cũ (max len).

Caller (scraper.py) đã có soup từ _sync_parse_and_clean → không parse lại.
"""
from __future__ import annotations

import re
from collections import Counter
from typing import TYPE_CHECKING
from urllib.parse import urlparse, unquote

from bs4 import BeautifulSoup

from utils.string_helpers import normalize_title

if TYPE_CHECKING:
    from ai.client import AIRateLimiter

_MIN_TITLE_LEN = 3

# Xóa phần ", a <fandom>" của fanfiction.net
_RE_FANDOM_TAG = re.compile(r",\s*a\s+.+$")
# Xóa suffix "Chapter N" ở cuối title
_RE_CHAP_SUFFIX = re.compile(r"\s+chapter\s+\d+.*$", re.IGNORECASE)


# ── TitleExtractor ────────────────────────────────────────────────────────────

class TitleExtractor:
    """
    Trích xuất tiêu đề chương từ BeautifulSoup bằng đa-nguồn + voting.

    Không cần state → có thể dùng như singleton.
    Không nhận `html: str` nữa — caller truyền soup đã parse để tránh
    parse lại lần thứ N trong cùng một chapter pipeline.

    FIX-D: Khi vote hòa, gọi ai_validate_title thay vì chọn theo max(len).
           ai_limiter là optional để không phá interface cũ.
    """

    async def extract(
        self,
        soup: BeautifulSoup,
        url: str,
        ai_limiter: "AIRateLimiter | None" = None,
    ) -> str:
        """
        Trích tiêu đề từ soup đã parse sẵn.

        Args:
            soup:       BeautifulSoup của trang chương (đã clean).
            url:        URL hiện tại — dùng làm fallback slug.
            ai_limiter: Nếu cung cấp, gọi AI khi vote hòa thay vì chọn max(len).
        """
        candidates = self._collect_candidates(soup, url)

        cleaned: list[str] = []
        for raw in candidates:
            if not raw:
                continue
            t = normalize_title(raw)
            if len(t) >= _MIN_TITLE_LEN:
                cleaned.append(t)

        if not cleaned:
            return self._from_url_slug(url) or "Không rõ tiêu đề"

        # Deduplicate case-insensitive, giữ lần xuất hiện đầu tiên
        lower_map: dict[str, str] = {}
        for t in cleaned:
            key = t.lower()
            if key not in lower_map:
                lower_map[key] = t

        counts = Counter(t.lower() for t in cleaned)
        top2   = counts.most_common(2)

        # Không hòa → trả về ngay
        if len(top2) == 1 or top2[0][1] != top2[1][1]:
            return lower_map[top2[0][0]]

        # ── Vote hòa ──────────────────────────────────────────────────────────
        tied = [lower_map[t[0]] for t in top2]

        # FIX-D: Dùng AI validate khi có ai_limiter
        if ai_limiter is not None:
            from ai.agents import ai_validate_title
            # Lấy snippet đầu trang để AI có context
            body_el = soup.find("body")
            snippet = body_el.get_text(separator=" ", strip=True)[:300] if body_el else ""

            # Thử validate candidate ngắn hơn trước (thường là tiêu đề thật)
            primary = min(tied, key=len)
            validated = await ai_validate_title(
                candidate       = primary,
                chapter_url     = url,
                content_snippet = snippet,
                ai_limiter      = ai_limiter,
            )
            if validated:
                return normalize_title(validated)

        # Fallback: chọn title dài nhất (hành vi cũ)
        return max(tied, key=len)

    def _collect_candidates(self, soup: BeautifulSoup, url: str) -> list[str]:
        result: list[str] = []

        for tag_name, attr in [
            ("title", None),
            ("meta",  "og:title"),
            ("h1",    None),
            ("h2",    None),
        ]:
            if attr:
                el = soup.find(tag_name, property=attr)
                if el and el.get("content"):
                    result.append(el["content"].strip())
            else:
                el = soup.find(tag_name)
                if el:
                    result.append(el.get_text(strip=True))

        prop = soup.find(attrs={"itemprop": "name"})
        if prop:
            result.append(prop.get_text(strip=True))

        for cls in ("chapter-title", "chap-title", "chapter_title", "entry-title"):
            el = soup.find(class_=cls)
            if el:
                result.append(el.get_text(strip=True))
                break

        slug = self._from_url_slug(url)
        if slug:
            result.append(slug)

        for sel in ("#chapter-c", "#chr-content", "div.chapter-content", "article"):
            content_div = soup.select_one(sel)
            if content_div:
                for tag in content_div.find_all(["h1", "h2", "h3", "strong"]):
                    text = tag.get_text(strip=True)
                    if len(text) > _MIN_TITLE_LEN:
                        result.append(text)
                        break
                break

        return result

    def _from_url_slug(self, url: str) -> str | None:
        try:
            path  = urlparse(url).path.rstrip("/")
            slug  = path.split("/")[-1]
            slug  = unquote(slug)
            if slug.isdigit():
                return None
            words = re.split(r"[-_]", slug)
            title = " ".join(w.capitalize() for w in words if w).strip()
            return title or None
        except Exception:
            return None


# ── Story title extraction ────────────────────────────────────────────────────

def extract_story_title(soup: BeautifulSoup, url: str) -> str | None:
    """
    Trích tên truyện (không phải tiêu đề chương) từ BeautifulSoup.

    Nguồn theo thứ tự ưu tiên:
      1. Breadcrumb — phần tử áp chót thường là tên truyện
      2. <title> dạng "Story Name Chapter N | SiteName"
    """
    # 1. Breadcrumb
    for bc in soup.find_all(attrs={"class": re.compile(r"breadcrumb", re.I)}):
        items = bc.find_all(["a", "span", "li"])
        if len(items) >= 2:
            candidate = items[-2].get_text(strip=True)
            if len(candidate) > 3:
                return normalize_title(candidate)

    # 2. <title> tag
    title_tag = soup.find("title")
    if title_tag:
        raw = title_tag.get_text(strip=True)
        if "|" in raw:
            before_pipe = raw.split("|")[0].strip()
            before_pipe = _RE_FANDOM_TAG.sub("",  before_pipe).strip()
            before_pipe = _RE_CHAP_SUFFIX.sub("", before_pipe).strip()
            if len(before_pipe) > 3:
                return normalize_title(before_pipe)

    return None