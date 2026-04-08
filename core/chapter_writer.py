"""
core/chapter_writer.py — Chapter filename formatting và content post-processing.

Fix P2-11: lru_cache cho _get_chapter_re() thay vì re.compile() trong hot path.
  format_chapter_filename() gọi re.compile() với cùng chapter_keyword pattern
  mỗi lần. 1000 chapters = 1000 compilations lãng phí vì chapter_keyword
  thường không đổi trong suốt 1 story (luôn là "Chapter", "Episode", v.v.).

  Sau: _get_chapter_re(chapter_kw) được cache bởi lru_cache(maxsize=32).
  maxsize=32 đủ cho edge cases (user scrape 32 stories với keyword khác nhau
  cùng lúc). Trong thực tế thường chỉ cần 2-3 entries.
"""
from __future__ import annotations

import functools
import re

from utils.string_helpers import slugify_filename
from utils.types import ProgressDict

# ── Constants ──────────────────────────────────────────────────────────────────

_RE_PIPE_SUFFIX = re.compile(r"\s*\|.*$")

_RE_WORD_COUNT = re.compile(
    r"^\[\s*[\d,.\s]+words?\s*\]$|^\[\s*\.+\s*words?\s*\]$",
    re.IGNORECASE,
)

_NAV_EDGE_SCAN = 7


# ── Cached regex factory ───────────────────────────────────────────────────────

@functools.lru_cache(maxsize=32)
def _get_chapter_re(chapter_kw: str) -> re.Pattern:
    """
    Compile và cache regex cho chapter keyword.

    Fix P2-11: gọi từ format_chapter_filename() — hot path, mỗi chapter.
    lru_cache đảm bảo mỗi keyword chỉ compile một lần duy nhất.

    Args:
        chapter_kw: keyword như "Chapter", "Episode", "Ch.", v.v.
                    Phải là str thuần (không có ký tự đặc biệt regex)
                    vì được escape bởi re.escape().
    """
    kw_esc = re.escape(chapter_kw)
    return re.compile(
        rf"(?:{kw_esc})\s*(?P<n>\d+)\s*[-–—:.]?\s*(?P<sub>.*)",
        re.IGNORECASE,
    )


# ── format_chapter_filename ────────────────────────────────────────────────────

def format_chapter_filename(
    chapter_num: int,
    raw_title  : str,
    progress   : ProgressDict,
) -> str:
    """
    Tạo tên file .md cho một chapter.

    Logic:
        1. Bóc story prefix nếu có (VD: "Monster Cultivator Chapter 5" → "Chapter 5")
        2. Parse chapter keyword + số (VD: "Chapter 5 – The Rise")
        3. Nếu has_subtitle=True → thêm subtitle vào filename
        4. Fallback: slugify toàn bộ title

    Args:
        chapter_num: Số thứ tự chapter trong progress (1-based)
        raw_title:   Title thô từ pipeline
        progress:    ProgressDict chứa naming rules từ Naming Phase
    """
    chapter_kw   = (progress.get("chapter_keyword") or "Chapter").strip()
    has_subtitle = bool(progress.get("has_chapter_subtitle", False))
    prefix_strip = (progress.get("story_prefix_strip") or "").strip()

    title = raw_title.strip()

    # Bóc story prefix
    if prefix_strip:
        lo_title  = title.lower()
        lo_prefix = prefix_strip.lower()
        if lo_title.startswith(lo_prefix):
            title = title[len(prefix_strip):].lstrip(" ,;:-–—")

    # Bóc pipe suffix
    title = _RE_PIPE_SUFFIX.sub("", title).strip()

    # Fix P2-11: dùng cached regex thay vì re.compile() mỗi lần
    m = _get_chapter_re(chapter_kw).search(title)

    if m:
        n       = m.group("n")
        sub_raw = m.group("sub").strip(" -–—:[]().")
        sub_raw = _RE_PIPE_SUFFIX.sub("", sub_raw).strip()
        chap_id = f"{chapter_kw}{n}"

        if has_subtitle and sub_raw and len(sub_raw) >= 2:
            sub_safe = slugify_filename(sub_raw, max_len=50)
            name     = f"{chapter_num:04d}_{chap_id}_{sub_safe}"
        else:
            name = f"{chapter_num:04d}_{chap_id}"
    else:
        fallback = (title or raw_title).strip()
        name     = f"{chapter_num:04d}_{slugify_filename(fallback, max_len=60)}"

    return slugify_filename(name, max_len=120) + ".md"


# ── strip_nav_edges ────────────────────────────────────────────────────────────

def strip_nav_edges(text: str) -> str:
    """
    Xóa navigation/boilerplate text ở đầu và cuối chapter content.

    Phát hiện:
        - Lines xuất hiện ở CẢ đầu VÀ cuối (repeated navigation)
        - "[1,234 words]" / "[... words]" patterns
        - Lines ngắn chỉ có chữ cái (Prev/Next/TOC labels)
    """
    lines = text.splitlines()
    n     = len(lines)

    if n < 8:
        return text

    EDGE    = _NAV_EDGE_SCAN
    top_set = {lines[i].strip() for i in range(min(EDGE, n)) if lines[i].strip()}
    bot_set = {lines[n-1-i].strip() for i in range(min(EDGE, n)) if lines[n-1-i].strip()}
    repeated = top_set & bot_set

    def _is_nav(line: str) -> bool:
        s = line.strip()
        if not s:
            return True
        if _RE_WORD_COUNT.match(s):
            return True
        if len(s) <= 10 and re.match(r"^[A-Za-z\s]+$", s):
            return True
        return s in repeated

    start = 0
    for i in range(min(EDGE, n)):
        if _is_nav(lines[i]):
            start = i + 1
        else:
            break
    while start < n and not lines[start].strip():
        start += 1

    end = n
    for i in range(min(EDGE, n)):
        idx = n - 1 - i
        if idx <= start:
            break
        if not lines[idx].strip() or _is_nav(lines[idx]):
            end = idx
        else:
            break
    while end > start and not lines[end-1].strip():
        end -= 1

    return "\n".join(lines[start:end]) if start < end else text