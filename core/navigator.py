# core/navigator.py
"""
core/navigator.py — Phát hiện URL chương tiếp theo và phân loại trang.

API thay đổi (breaking change có chủ ý):
  find_next_url(soup, url, profile)   — nhận BeautifulSoup thay vì str
  detect_page_type(soup, url)         — nhận BeautifulSoup thay vì str

Lý do: scraper.py đã parse + clean HTML trong asyncio.to_thread() trước khi
gọi các hàm này. Truyền soup object trực tiếp tránh parse lại lần 2.
Caller chịu trách nhiệm tạo soup (thường qua _sync_parse_and_clean).
"""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from config import (
    RE_NEXT_BTN,
    RE_CHAP_SLUG,
    RE_CHAP_URL,
    RE_CHAP_HREF,
    RE_CHAP_KW_URL,
)

# fanfiction.net: /s/{story_id}/{chapter_num}/{optional_slug}
_RE_FANFIC_CHAPTER = re.compile(r"(/s/\d+/)(\d+)(/.+)?$")


def find_next_url(
    soup: BeautifulSoup,
    current_url: str,
    profile: dict,
) -> str | None:
    """
    Tìm URL chương tiếp theo bằng heuristic (không gọi AI).

    Nhận BeautifulSoup đã được clean (remove_hidden_elements) từ caller.

    Thứ tự ưu tiên:
      1. CSS selector từ site profile (học được trước đó)
      2. <link rel="next"> hoặc <a rel="next">
      3. Anchor text chứa "next / tiếp / sau / siguiente"
      4. <select> dropdown danh sách chương
      5. Tăng số chương trong URL slug  (/chapter-12 → /chapter-13)
      6. fanfiction.net pattern  (/s/123/5/ → /s/123/6/)
    """
    base = current_url

    # 1. Profile selector (độ chính xác cao nhất)
    next_sel = profile.get("next_selector")
    if next_sel:
        el = soup.select_one(next_sel)
        if el and el.get("href"):
            return urljoin(base, el["href"])

    # 2. Semantic rel="next"
    rel_next = soup.find("link", rel="next") or soup.find("a", rel="next")
    if rel_next and rel_next.get("href"):
        return urljoin(base, rel_next["href"])

    # 3. Anchor text "Next / Tiếp"
    for a in soup.find_all("a", href=True):
        if RE_NEXT_BTN.search(a.get_text(strip=True)):
            return urljoin(base, a["href"])

    # 4. Dropdown <select> chương
    for sel_tag in soup.find_all("select"):
        options = sel_tag.find_all("option")
        for i, opt in enumerate(options):
            href = opt.get("value", "")
            if href and current_url.endswith(href.lstrip("/")):
                if i + 1 < len(options):
                    next_val = options[i + 1].get("value", "")
                    if next_val:
                        return urljoin(base, next_val)

    # 5. Tăng số trong slug URL
    m = RE_CHAP_SLUG.search(current_url)
    if m:
        return f"{m.group(1)}{int(m.group(2)) + 1}{m.group(3)}"

    # 6. fanfiction.net chapter index
    m = _RE_FANFIC_CHAPTER.search(current_url)
    if m:
        return (
            current_url[: m.start()]
            + m.group(1)
            + str(int(m.group(2)) + 1)
            + (m.group(3) or "")
        )

    return None


def detect_page_type(soup: BeautifulSoup, url: str) -> str:
    """
    Phân loại trang: 'chapter' | 'index' | 'other'.

    Nhận BeautifulSoup từ caller — không parse lại HTML.
    Score-based: cộng điểm cho từng tín hiệu, lấy bên thắng.
    Trả về 'other' khi hòa.
    """
    score: dict[str, int] = {"chapter": 0, "index": 0}

    # URL chứa pattern chương → nghiêng về chapter
    if RE_CHAP_URL.search(url):
        score["chapter"] += 2

    anchors = soup.find_all("a")

    for a in anchors:
        text = a.get_text(strip=True)
        href = a.get("href", "")
        if RE_NEXT_BTN.search(text):
            score["chapter"] += 1
        if RE_CHAP_HREF.search(href):
            score["index"] += 1

    # Nhiều link chứa từ khoá chương → index / mục lục
    chap_links = sum(1 for a in anchors if RE_CHAP_KW_URL.search(a.get_text()))
    if chap_links > 5:
        score["index"] += 2
    elif chap_links > 1:
        score["index"] += 1

    if score["chapter"] > score["index"]:
        return "chapter"
    if score["index"] > score["chapter"]:
        return "index"
    return "other"