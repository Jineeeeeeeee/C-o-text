# core/html_filter.py
"""
core/html_filter.py — Xóa elements ẩn, noise, và remove_selectors từ profile.

Pipeline:
  1. Thu thập dynamic hidden classes từ <style> (trước khi xóa)
  2. strip_noise_tags: xóa script/style/noscript/iframe/svg...
  3. remove_hidden_elements: xóa hidden attr, aria-hidden, CSS display:none
  4. remove_profile_selectors: xóa elements theo remove_selectors từ profile
     ⚠ Bỏ qua nếu element nằm BÊN TRONG content_selector (tránh tự nuking content)
"""
from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_NOISE_TAGS = frozenset({
    "script", "style", "noscript", "iframe",
    "svg", "canvas", "picture", "source",
    "video", "audio", "form",
})

_HIDDEN_STYLE_RE = re.compile(
    r"display\s*:\s*none"
    r"|visibility\s*:\s*hidden"
    r"|opacity\s*:\s*0(?:\.0+)?\b"
    r"|font-size\s*:\s*0"
    r"|color\s*:\s*transparent"
    r"|width\s*:\s*0"
    r"|height\s*:\s*0",
    re.IGNORECASE,
)

_HIDDEN_CLASS_RE = re.compile(
    r"\b(?:"
    r"hidden|invisible|sr-only|visually-hidden|"
    r"d-none|display-none|hide|offscreen|"
    r"watermark|wm-text|noshow|no-show|"
    r"rr-hidden|rr-copyright|sh-notice|"
    r"theft-notice|stolen-notice|copyright-notice"
    r")\b",
    re.IGNORECASE,
)

_CSS_HIDDEN_RULE_RE = re.compile(
    r"\.([\w-]{4,})\s*\{[^}]*"
    r"(?:display\s*:\s*none|speak\s*:\s*never|visibility\s*:\s*hidden)"
    r"[^}]*\}",
    re.IGNORECASE | re.DOTALL,
)


def prepare_soup(
    html: str,
    remove_selectors: list[str] | None = None,
    content_selector: str | None = None,
) -> BeautifulSoup:
    """
    Parse HTML và chạy full cleaning pipeline.

    Args:
        html:              Raw HTML string
        remove_selectors:  CSS selectors từ profile để xóa (VD: [".ads", ".donate-btn"])
        content_selector:  CSS selector của content area — các element BÊN TRONG
                           vùng này sẽ KHÔNG bị xóa bởi remove_selectors.
                           Ngăn trường hợp AI học selector như "div#storytext > div"
                           vô tình xóa luôn nội dung truyện.

    Returns:
        Cleaned BeautifulSoup object
    """
    soup = BeautifulSoup(html, "html.parser")

    # Bước 1: Thu thập dynamic hidden classes TRƯỚC khi xóa <style>
    dynamic_hidden = _extract_css_hidden_classes(soup)

    # Bước 2: Xóa noise tags
    for tag in _NOISE_TAGS:
        for el in list(soup.find_all(tag)):
            el.decompose()

    # Bước 3: Xóa hidden elements
    for el in list(soup.find_all(True)):
        if not isinstance(el, Tag):
            continue
        if _is_hidden(el, dynamic_hidden):
            el.decompose()

    # Bước 4: Xóa profile-specified selectors
    # ── QUAN TRỌNG: bỏ qua nếu element nằm trong content_selector ────────────
    if remove_selectors:
        content_el: Tag | None = None
        if content_selector:
            try:
                content_el = soup.select_one(content_selector)
            except Exception:
                pass

        for sel in remove_selectors:
            try:
                for el in list(soup.select(sel)):
                    # Không xóa content element hoặc element con của nó
                    if content_el is not None and (
                        el is content_el
                        or content_el in el.parents
                    ):
                        continue
                    el.decompose()
            except Exception:
                pass

    return soup


def _extract_css_hidden_classes(soup: BeautifulSoup) -> frozenset[str]:
    hidden: set[str] = set()
    for style in soup.find_all("style"):
        css = style.get_text()
        if css:
            for m in _CSS_HIDDEN_RULE_RE.finditer(css):
                hidden.add(m.group(1))
    return frozenset(hidden)


def _is_hidden(el: Tag, dynamic_hidden: frozenset[str]) -> bool:
    if not el.attrs:
        return False
    if el.has_attr("hidden"):
        return True
    if el.get("aria-hidden") == "true":
        return True
    style = el.get("style", "")
    if style and _HIDDEN_STYLE_RE.search(style):
        return True
    classes = " ".join(el.get("class", []))
    if classes and _HIDDEN_CLASS_RE.search(classes):
        return True
    if dynamic_hidden:
        el_classes = el.get("class") or []
        if any(c in dynamic_hidden for c in el_classes):
            return True
    return False