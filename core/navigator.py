"""
core/navigator.py — Tìm URL chương tiếp theo (không AI).

Simplified: chỉ còn 5 strategies, nav_type từ profile dùng làm fast path.
Cũng chứa heuristic phát hiện Index vs Chapter page.
"""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from config import RE_NEXT_BTN, RE_CHAP_SLUG, RE_CHAP_URL, RE_CHAP_HREF, RE_CHAP_KW, RE_FANFIC
from utils.types import SiteProfile


def find_next_url(
    soup: BeautifulSoup,
    current_url: str,
    profile: SiteProfile,
) -> str | None:
    """
    Tìm URL chương tiếp theo bằng heuristic.

    Fast path: nếu profile có nav_type đã biết → thử strategy tương ứng trước.
    Fallback: thử tất cả theo thứ tự ưu tiên.
    """
    nav_type = profile.get("nav_type")
    if nav_type:
        result = _try_nav_type(soup, current_url, profile, nav_type)
        if result:
            return result

    return _try_all(soup, current_url, profile)


def _try_nav_type(soup: BeautifulSoup, base: str, profile: SiteProfile, nav_type: str) -> str | None:
    dispatch = {
        "selector"       : lambda: _try_selector(soup, base, profile),
        "rel_next"       : lambda: _try_rel_next(soup, base),
        "slug_increment" : lambda: _try_slug(base),
        "fanfic"         : lambda: _try_fanfic(soup, base),
    }
    fn = dispatch.get(nav_type)
    return fn() if fn else None


def _try_all(soup: BeautifulSoup, base: str, profile: SiteProfile) -> str | None:
    return (
        _try_selector(soup, base, profile)
        or _try_rel_next(soup, base)
        or _try_anchor_text(soup, base)
        or _try_slug(base)
        or _try_fanfic(soup, base)
    )


def _try_selector(soup: BeautifulSoup, base: str, profile: SiteProfile) -> str | None:
    sel = profile.get("next_selector")
    if not sel:
        return None
    try:
        el = soup.select_one(sel)
        if el and el.get("href"):
            return urljoin(base, el["href"])
    except Exception:
        pass
    return None


def _try_rel_next(soup: BeautifulSoup, base: str) -> str | None:
    el = soup.find("link", rel="next") or soup.find("a", rel="next")
    if el and el.get("href"):
        return urljoin(base, el["href"])
    return None


def _try_anchor_text(soup: BeautifulSoup, base: str) -> str | None:
    for a in soup.find_all("a", href=True):
        if RE_NEXT_BTN.search(a.get_text(strip=True)):
            return urljoin(base, a["href"])
    return None


def _try_slug(base: str) -> str | None:
    m = RE_CHAP_SLUG.search(base)
    if m:
        return f"{m.group(1)}{int(m.group(2)) + 1}{m.group(3)}"
    return None


def _try_fanfic(soup: BeautifulSoup, base: str) -> str | None:
    m = RE_FANFIC.search(base)
    if m:
        return base[: m.start()] + m.group(1) + str(int(m.group(2)) + 1) + (m.group(3) or "")
    return None


# ── Page type detection ───────────────────────────────────────────────────────

def detect_page_type(soup: BeautifulSoup, url: str) -> str:
    """
    Phân loại trang: 'chapter' | 'index' | 'other'.
    Score-based — không AI.
    """
    score: dict[str, int] = {"chapter": 0, "index": 0}

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

    chap_links = sum(1 for a in anchors if RE_CHAP_KW.search(a.get_text()))
    if chap_links > 5:
        score["index"] += 2
    elif chap_links > 1:
        score["index"] += 1

    if score["chapter"] > score["index"]:
        return "chapter"
    if score["index"] > score["chapter"]:
        return "index"
    return "other"