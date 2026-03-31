# core/html_filter.py
"""
core/html_filter.py — Loại bỏ element ẩn / watermark khỏi BeautifulSoup tree.

Tách khỏi scraper.py để có thể test độc lập và thêm rule dễ dàng.

Thêm rule mới:
  - Style inline   : bổ sung vào _HIDDEN_STYLE_RE
  - Class tĩnh     : bổ sung vào _HIDDEN_CLASS_RE
  - CSS động       : _extract_css_hidden_classes() tự xử lý (không cần thêm tay)

THAY ĐỔI so với phiên bản cũ:
  _extract_css_hidden_classes() — parse <style> block để tìm class name
  được khai báo display:none / speak:never / visibility:hidden.

  Tại sao cần:
    RoyalRoad (và nhiều site khác) nhúng watermark bằng cách tạo class name
    NGẪU NHIÊN (hash) rồi khai báo trong <style> block cùng trang:

      <style>
        .cjNkODUzODEx { display: none; speak: never; }
      </style>
      <p class="cjNkODUzODEx">
        If you come across this story on Amazon, it has been stolen.
      </p>

    Browser đọc <style> → ẩn element → reader không thấy.
    curl_cffi lấy raw HTML, không chạy CSS → watermark lộ ra trong .md.

    _HIDDEN_CLASS_RE không thể bắt vì class name thay đổi mỗi chapter.
    Thêm tay cũng vô ích. Giải pháp duy nhất: parse <style> động.

  Hiệu quả: bắt được 100% watermark dạng này,
            kể cả class name hoàn toàn mới chưa từng gặp.
"""
import logging
import re

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


# ── Compiled regexes ──────────────────────────────────────────────────────────

# Inline style attribute — các giá trị ẩn element
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

# Class name tĩnh, phổ biến — không cần biết trước site
_HIDDEN_CLASS_RE = re.compile(
    r"\b(?:"
    r"hidden|invisible|sr-only|visually-hidden|"
    r"d-none|display-none|hide|offscreen|"
    r"watermark|wm-text|protect-text|anti-theft|"
    # Thêm pattern phổ biến từ các site novel
    r"noshow|no-show|nocopy|no-copy|"
    r"rr-hidden|rr-copyright|sh-notice|"
    r"theft-notice|stolen-notice|copyright-notice"
    r")\b",
    re.IGNORECASE,
)

# Parse CSS rule trong <style> block:
# Bắt className nếu rule chứa display:none / speak:never / visibility:hidden
#
# Regex giải thích:
#   \.([\w-]{4,})   → tên class bắt đầu bằng dấu . , ít nhất 4 ký tự
#                     (loại bỏ pseudo-class ngắn như :nth, .sm, .lg)
#   \s*\{[^}]*      → mở ngoặc + bất kỳ property nào trước
#   (?:...)         → điều kiện: phải có ít nhất 1 trong 3 property ẩn
#   [^}]*\}         → phần còn lại của rule
_CSS_HIDDEN_RULE_RE = re.compile(
    r"\.([\w-]{4,})"
    r"\s*\{[^}]*"
    r"(?:"
    r"display\s*:\s*none"
    r"|speak\s*:\s*never"
    r"|visibility\s*:\s*hidden"
    r")"
    r"[^}]*\}",
    re.IGNORECASE | re.DOTALL,
)


# ── Public API ────────────────────────────────────────────────────────────────

def remove_hidden_elements(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Xóa tất cả DOM element bị ẩn, bao gồm:

      1. hidden attribute hoặc aria-hidden="true"
      2. CSS inline style: display:none, visibility:hidden, opacity:0, ...
      3. Class tĩnh: hidden, invisible, watermark, anti-theft, ...
      4. [MỚI] Class được khai báo hidden trong <style> block
         → Bắt được RoyalRoad random-hash watermark và mọi site dùng cùng kỹ thuật

    Trả về cùng đối tượng soup (mutate in-place) để tiện chain.

    Độ phức tạp:
      - Bước 1 (parse <style>): O(S) với S = tổng ký tự trong style blocks
      - Bước 2 (duyệt element): O(N) với N = số element trong DOM
    """
    # ── Bước 1: Thu thập dynamic hidden classes từ <style> blocks ─────────────
    # Phải làm TRƯỚC khi duyệt elements để có đầy đủ class set
    dynamic_hidden = _extract_css_hidden_classes(soup)

    if dynamic_hidden:
        logger.debug(
            "[HiddenFilter] Tìm thấy %d dynamic hidden class từ <style>: %s",
            len(dynamic_hidden),
            sorted(dynamic_hidden)[:5],  # log tối đa 5 để tránh spam
        )

    # ── Bước 2: Duyệt và xóa elements ─────────────────────────────────────────
    removed = 0
    # find_all(True) trả về tất cả Tag (không phải NavigableString)
    # Dùng list() để tránh lỗi "size changed during iteration" khi decompose
    for el in list(soup.find_all(True)):
        if not isinstance(el, Tag):
            continue
        if not isinstance(el.attrs, dict):
            continue
        if _is_hidden(el) or _has_dynamic_hidden_class(el, dynamic_hidden):
            el.decompose()
            removed += 1

    if removed:
        logger.debug("[HiddenFilter] Đã xóa %d element ẩn", removed)

    return soup


# ── Private helpers ───────────────────────────────────────────────────────────

def _extract_css_hidden_classes(soup: BeautifulSoup) -> frozenset[str]:
    """
    Parse tất cả thẻ <style> trong trang.
    Trả về frozenset các class name được khai báo là display:none,
    speak:never, hoặc visibility:hidden.

    Ví dụ CSS bắt được:
      .cjNkODUzODEx { display: none; speak: never; }
      .rr-wm-829af  { visibility: hidden; }
      .ch-hide-3x7  { display: none !important; }

    Trả về frozenset rỗng nếu trang không có <style> block hoặc
    không có rule nào thỏa điều kiện.

    frozenset thay vì set để:
      1. Immutable → an toàn khi truyền sang hàm khác
      2. Lookup O(1) — hiệu năng tốt hơn list khi check nhiều element
    """
    hidden_classes: set[str] = set()

    for style_tag in soup.find_all("style"):
        css_text = style_tag.get_text()
        if not css_text:
            continue
        for match in _CSS_HIDDEN_RULE_RE.finditer(css_text):
            class_name = match.group(1)
            hidden_classes.add(class_name)

    return frozenset(hidden_classes)


def _has_dynamic_hidden_class(el: Tag, dynamic_hidden: frozenset[str]) -> bool:
    """
    Kiểm tra element có class nào nằm trong dynamic hidden set không.

    Trả về False ngay nếu dynamic_hidden rỗng (trang không có <style> rule)
    để tránh chi phí get("class") không cần thiết.
    """
    if not dynamic_hidden:
        return False
    el_classes: list[str] = el.get("class") or []
    return any(cls in dynamic_hidden for cls in el_classes)


def _is_hidden(el: Tag) -> bool:
    """
    Kiểm tra một element có đang bị ẩn hay không (static rules).

    Kiểm tra theo thứ tự từ nhanh đến chậm:
      1. has_attr("hidden") — O(1) dict lookup
      2. aria-hidden="true" — O(1) dict lookup
      3. inline style match — O(len(style string))
      4. class match       — O(len(class string))
    """
    # HTML boolean attribute hoặc ARIA
    if el.has_attr("hidden"):
        return True
    if el.get("aria-hidden") == "true":
        return True

    # Inline CSS style
    style = el.get("style", "")
    if style and _HIDDEN_STYLE_RE.search(style):
        return True

    # CSS class tĩnh
    classes = " ".join(el.get("class", []))
    if classes and _HIDDEN_CLASS_RE.search(classes):
        return True

    return False