from __future__ import annotations

import re
from typing import List


# ── Thresholds ─────────────────────────────────────────────────────────────────

_MIN_REMAINING   = 100
_MIN_PROSE_WORDS = 7
_MAX_STRIP_RATIO = 0.60


# ── Pass 0: Raw script/HTML lines (NEW) ───────────────────────────────────────
#
# Một số sites (VD: NovelFire) inject <script> tags dưới dạng TEXT NODE bên trong
# content div. BeautifulSoup parse chúng thành NavigableString (không phải Tag),
# nên _EXTRACT_SKIP_TAGS và prepare_soup() đều không bắt được.
# Kết quả: script tag text xuất hiện verbatim trong extracted content.
#
# Pattern: line bắt đầu bằng "<script" (sau khi strip whitespace).
# Không dùng broad HTML regex để tránh false positive với
# nội dung truyện chứa ký tự < (VD: "< 5 minutes", math expressions).
#
_RAW_SCRIPT_LINE_RE = re.compile(r"^\s*<script\b", re.IGNORECASE)


def _strip_raw_script_lines(text: str) -> str:
    """
    Strip lines that are raw <script> tag content rendered as text.

    Chỉ strip lines BẮT ĐẦU bằng <script — không strip content truyện
    có thể chứa < ở giữa câu.
    """
    lines = text.splitlines()
    result = []
    for line in lines:
        if _RAW_SCRIPT_LINE_RE.match(line):
            continue
        result.append(line)

    candidate = "\n".join(result)
    return candidate if len(candidate.strip()) >= _MIN_REMAINING else text


# ── Pass 1: Comment section ────────────────────────────────────────────────────

_COMMENT_MARKERS = [
    re.compile(r"begin\s+comments?",        re.I),
    re.compile(r"^comments?\s*\(\d+\)\s*$", re.I),
    re.compile(r"^log\s+in\s+to\s+comment\s*$", re.I),
    re.compile(r"^write\s+a\s+review\s*$",  re.I),
    re.compile(r"^post\s+a\s+comment\s*$",  re.I),
    re.compile(r"^leave\s+a\s+comment\s*$", re.I),
]


def _strip_comment_section(text: str) -> str:
    lines  = text.splitlines()
    n      = len(lines)
    cutoff = max(5, int(n * 0.30))

    for i, line in enumerate(lines):
        if i < cutoff:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if any(p.search(stripped) for p in _COMMENT_MARKERS):
            following  = [l.strip() for l in lines[i + 1: i + 7] if l.strip()]
            prose_count = sum(
                1 for l in following
                if len(l.split()) >= _MIN_PROSE_WORDS
                and not any(p.search(l) for p in _COMMENT_MARKERS)
            )
            if prose_count <= 1:
                candidate = "\n".join(lines[:i])
                if len(candidate.strip()) >= _MIN_REMAINING:
                    return candidate
    return text


# ── Pass 2: Settings panel ─────────────────────────────────────────────────────

_SETTINGS_EXACT = frozenset({
    "font size", "font family", "font color", "font",
    "color", "color scheme", "theme",
    "background", "dim background",
    "reader width", "width", "line spacing", "paragraph spacing",
    "reading mode", "reading options",
    "expand", "tighten",
    "3/4", "1/2",
})

_SETTINGS_PREFIX = (
    "theme (", "font size", "font family",
    "reading settings", "display settings", "site settings",
)


def _is_settings_line(line: str) -> bool:
    lo = line.strip().lower()
    if not lo:
        return False
    if lo in _SETTINGS_EXACT:
        return True
    if any(lo.startswith(sw) for sw in _SETTINGS_PREFIX):
        return True
    return False


def _strip_settings_panel(text: str) -> str:
    lines  = text.splitlines()
    result : List[str] = []
    i      = 0

    while i < len(lines):
        window_size    = min(8, len(lines) - i)
        window         = lines[i: i + window_size]
        settings_count = sum(1 for l in window if _is_settings_line(l))

        if settings_count >= 4:
            j             = i + window_size
            prose_streak  = 0
            while j < len(lines):
                l = lines[j].strip()
                if not l:
                    j += 1
                    continue
                if not _is_settings_line(lines[j]):
                    prose_streak += 1
                    if prose_streak >= 2:
                        break
                else:
                    prose_streak = 0
                j += 1
            i = j
        else:
            result.append(lines[i])
            i += 1

    candidate = "\n".join(result)
    return candidate if len(candidate.strip()) >= _MIN_REMAINING else text


# ── Pass 3: Postfix support/nav section ───────────────────────────────────────

_POSTFIX_SECTION_MARKERS = [
    re.compile(r"^#{1,6}\s+support\b",           re.I),
    re.compile(r"^#{1,6}\s+about\s+the\s+author", re.I),
    re.compile(r"^#{1,6}\s+author.{0,20}note",   re.I),
    re.compile(r"^-{3,}\s*$"),
]

_NAV_CLUSTER_WORDS = frozenset({
    "previous", "prev", "next", "fiction", "chapter",
    "home", "contents", "toc", "index", "donate", "patreon",
    "report", "subscribe",
})

_NAV_CLUSTER_THRESHOLD = 3


def _strip_postfix_section(text: str) -> str:
    lines  = text.splitlines()
    n      = len(lines)
    cutoff = max(3, int(n * 0.35))

    for i, line in enumerate(lines):
        if i < cutoff:
            continue
        stripped = line.strip()

        if any(p.search(stripped) for p in _POSTFIX_SECTION_MARKERS[:2]):
            candidate = "\n".join(lines[:i])
            if len(candidate.strip()) >= _MIN_REMAINING:
                return candidate

        window = [l.strip().lower() for l in lines[i: i + 5] if l.strip()]
        nav_hits = sum(1 for w in window if w in _NAV_CLUSTER_WORDS)
        if nav_hits >= _NAV_CLUSTER_THRESHOLD:
            candidate = "\n".join(lines[:i])
            if len(candidate.strip()) >= _MIN_REMAINING:
                return candidate

    return text


# ── Pass 4: Story metadata header ─────────────────────────────────────────────

_META_RE = [
    re.compile(r"^by\s*:?\s*\S",               re.I),
    re.compile(r"^by\s*$",                     re.I),
    re.compile(
        r"\b(?:words?|chapters?|reviews?|favs?|favorites?|follows?)\s*:",
        re.I,
    ),
    re.compile(r"\b(?:updated|published|posted)\s*:", re.I),
    re.compile(r"^\s*id\s*:\s*\d+\s*$",        re.I),
    re.compile(r"^fiction\s+[TKM]\b",           re.I),
    re.compile(r"^rated\s*:",                    re.I),
    re.compile(
        r"[-–]\s*(?:english|french|spanish|japanese|korean|chinese)\s*[-–]",
        re.I,
    ),
    re.compile(r"\d{1,3},\d{3}\s+words?\b",     re.I),
    re.compile(r"^(?:genre|category|status)\s*:", re.I),
    re.compile(r"^fiction\s+page\s*$",           re.I),
    re.compile(r"^donate\s*$",                   re.I),
    re.compile(r"^report\s+chapter\s*$",         re.I),
    re.compile(r"^#{1,6}\s*$"),
]


def _strip_metadata_header(text: str) -> str:
    lines    = text.splitlines()
    meta_end = 0
    in_block = False

    for i, line in enumerate(lines[:25]):
        stripped = line.strip()
        if not stripped:
            if in_block:
                meta_end = i + 1
            continue

        is_meta    = any(p.search(stripped) for p in _META_RE)
        is_list_meta = (
            stripped.startswith("-")
            and len(stripped) <= 100
            and in_block
        )
        is_artifact = (
            in_block
            and len(stripped) <= 8
            and re.match(r"^[\d+/\-.,\*#]+$", stripped)
        )

        if is_meta or is_list_meta or is_artifact:
            if not in_block:
                in_block = True
            meta_end = i + 1
        elif in_block:
            if len(stripped.split()) >= _MIN_PROSE_WORDS:
                break
            else:
                meta_end = i + 1

    if meta_end >= 3 and in_block:
        while meta_end < len(lines) and not lines[meta_end].strip():
            meta_end += 1
        candidate = "\n".join(lines[meta_end:])
        if len(candidate.strip()) >= _MIN_REMAINING:
            return candidate

    return text


# ── Pass 5: Author bio ─────────────────────────────────────────────────────────

_BIO_RE = [
    re.compile(r"^\*+\s*bio\s*\*+\s*$",           re.I),
    re.compile(r"^[\*\s]*\bbio\b[\*\s:]*$",        re.I),
    re.compile(r"^achievements?\s*$",              re.I),
    re.compile(r"^follow\s+(?:the\s+)?author",     re.I),
    re.compile(r"^end\s+col-md-",                  re.I),
    re.compile(r"^end\s+row\s*$",                  re.I),
    re.compile(r"^\#\s+\w[\w\s]*$",               re.I),
    re.compile(r"^-\s+\*\*\s+\w{3}",              re.I),
    re.compile(r"^\w+\s+Lakes?\s+sect\s*$",        re.I),
]


def _strip_author_bio(text: str) -> str:
    lines  = text.splitlines()
    n      = len(lines)
    cutoff = max(5, int(n * 0.55))

    first_marker = None
    for i in range(n - 1, cutoff - 1, -1):
        stripped = lines[i].strip()
        if not stripped:
            continue
        if any(p.search(stripped) for p in _BIO_RE):
            first_marker = i
            break

    if first_marker is None:
        return text

    cut_pos           = first_marker
    consecutive_prose = 0
    scan_limit        = max(cutoff - 1, first_marker - 50)

    for i in range(first_marker - 1, scan_limit, -1):
        stripped = lines[i].strip()

        if not stripped:
            continue

        is_bio_marker  = any(p.search(stripped) for p in _BIO_RE)
        is_short_noise = len(stripped.split()) <= 4 and not re.search(r"[.!?]$", stripped)

        if is_bio_marker or is_short_noise:
            cut_pos           = i
            consecutive_prose = 0
        else:
            consecutive_prose += 1
            if consecutive_prose >= 2:
                break

    candidate = "\n".join(lines[:cut_pos])
    if len(candidate.strip()) >= _MIN_REMAINING:
        return candidate
    return text


# ── Pass 6: Static UI navigation text patterns ────────────────────────────────
#
# Bổ sung cho ads_filter (dynamic). Pass này là static — hardcoded patterns phổ biến.
# Belt-and-suspenders: ads_filter học từ dữ liệu, pass này là safety net.

_UI_NAV_PATTERNS: list[re.Pattern] = [
    re.compile(r"^restore scroll position\s*$",                     re.I),
    re.compile(r"^tap the middle of the screen to reveal",          re.I),
    re.compile(r"^tip\s*:\s*you can use left.*right.*keyboard",     re.I),
    re.compile(r"^share to your friends\s*$",                       re.I),
    re.compile(r"^if you find any errors.*let us know\s*",          re.I),
    re.compile(r"^report chapter\s*$",                              re.I),
    re.compile(r"^report error\s*$",                                re.I),
    re.compile(r"^support the (author|translator|series)\s*$",      re.I),
    re.compile(r"^add to library\s*$",                              re.I),
    re.compile(r"^send gift\s*$",                                   re.I),
    re.compile(r"^vote for this chapter\s*$",                       re.I),
    re.compile(r"^unlock.*chapter\s*$",                             re.I),
    re.compile(r"^locked chapter\s*$",                              re.I),
    re.compile(r"^read more at\b",                                  re.I),
    re.compile(r"^visit.*for the latest",                           re.I),
    re.compile(r"^the source of this content is\b",                 re.I),
    re.compile(r"^this content is taken from",                      re.I),
    re.compile(r"^please read this on the (original|official)",     re.I),
    re.compile(r"^if you want to read more chapters.*follow.*on",   re.I),
]


def _strip_ui_navigation_text(text: str) -> str:
    """
    Strip các dòng là UI navigation text phổ biến (static patterns).

    Không có cutoff threshold — các pattern này rất đặc tưng, ít false positive.
    """
    if not text:
        return text
    lines = text.splitlines()
    result = [line for line in lines
              if not any(p.match(line.strip()) for p in _UI_NAV_PATTERNS)]
    candidate = "\n".join(result)
    return candidate if len(candidate.strip()) >= _MIN_REMAINING else text


# ── Main entry point ──────────────────────────────────────────────────────────

def clean_extracted_content(text: str) -> str:
    """
    Apply tất cả cleaning passes theo thứ tự.

    Pass order:
        0. _strip_raw_script_lines  (NEW — <script> text nodes từ sites như NovelFire)
        1. _strip_comment_section   (từ 30% trở xuống)
        2. _strip_settings_panel    (bất kỳ vị trí)
        3. _strip_postfix_section   (từ 35% trở xuống)
        4. _strip_metadata_header   (25 dòng đầu)
        5. _strip_author_bio        (từ 55% trở xuống)
        6. _strip_ui_navigation_text (static UI patterns — bất kỳ vị trí)

    Conservative: không bao giờ return ít hơn 40% original content.
    """
    if not text or len(text.strip()) < _MIN_REMAINING:
        return text

    original_len = len(text.strip())
    result       = text

    result = _strip_raw_script_lines(result)    # Pass 0 (NEW)
    result = _strip_comment_section(result)     # Pass 1
    result = _strip_settings_panel(result)      # Pass 2
    result = _strip_postfix_section(result)     # Pass 3
    result = _strip_metadata_header(result)     # Pass 4
    result = _strip_author_bio(result)          # Pass 5
    result = _strip_ui_navigation_text(result)  # Pass 6 (NEW)

    cleaned_len = len(result.strip())

    # Safety: nếu strip > 60% → return original
    if cleaned_len < original_len * (1 - _MAX_STRIP_RATIO):
        return text

    return result.strip() if result.strip() else text