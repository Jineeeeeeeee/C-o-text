

from __future__ import annotations

import re
from typing import List


# ── Thresholds ─────────────────────────────────────────────────────────────────

_MIN_REMAINING   = 100
_MIN_PROSE_WORDS = 7
_MAX_STRIP_RATIO = 0.60


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


# ── Pass 3 (NEW): Postfix support/nav section ──────────────────────────────────

# Explicit section markers that indicate "content is done, post-chapter footer starts"
_POSTFIX_SECTION_MARKERS = [
    re.compile(r"^#{1,6}\s+support\b",           re.I),   # "##### Support 'Story'"
    re.compile(r"^#{1,6}\s+about\s+the\s+author", re.I),  # "## About the author"
    re.compile(r"^#{1,6}\s+author.{0,20}note",   re.I),   # "## Author's Note" (at end)
    re.compile(r"^-{3,}\s*$"),                             # "---" divider (standalone)
]

# Words that appear as standalone nav labels in post-chapter footer
_NAV_CLUSTER_WORDS = frozenset({
    "previous", "prev", "next", "fiction", "chapter",
    "home", "contents", "toc", "index", "donate", "patreon",
    "report", "subscribe",
})

# Minimum nav words in a 5-line window to classify as nav cluster
_NAV_CLUSTER_THRESHOLD = 3


def _strip_postfix_section(text: str) -> str:
    lines  = text.splitlines()
    n      = len(lines)
    cutoff = max(3, int(n * 0.35))

    for i, line in enumerate(lines):
        if i < cutoff:
            continue
        stripped = line.strip()

        # Check explicit section markers
        # NOTE: standalone "---" only triggers if surrounded by non-prose context
        # (within 3 lines of a nav cluster or explicit marker)
        if any(p.search(stripped) for p in _POSTFIX_SECTION_MARKERS[:2]):
            # "Support" or "About the author" headings → cut immediately
            candidate = "\n".join(lines[:i])
            if len(candidate.strip()) >= _MIN_REMAINING:
                return candidate

        # Check nav cluster in upcoming window
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
    re.compile(r"^by\s*$",                     re.I),   # Fix CLEANER-C: standalone "by"
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
    # Fix CLEANER-C: Royal Road nav items injected at top of wide selector
    re.compile(r"^fiction\s+page\s*$",           re.I),
    re.compile(r"^donate\s*$",                   re.I),
    re.compile(r"^report\s+chapter\s*$",         re.I),
    # Fix CLEANER-C: empty heading artifact "####" from MarkdownFormatter
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
            and re.match(r"^[\d+/\-.,\*#]+$", stripped)  # Fix: include # for heading artifacts
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
    # Fix CLEANER-C: "** **Bio:**" = Royal Road rendered markdown variant
    re.compile(r"^[\*\s]*\bbio\b[\*\s:]*$",        re.I),
    re.compile(r"^achievements?\s*$",              re.I),
    re.compile(r"^follow\s+(?:the\s+)?author",     re.I),
    re.compile(r"^end\s+col-md-",                  re.I),
    re.compile(r"^end\s+row\s*$",                  re.I),
    re.compile(r"^\#\s+\w[\w\s]*$",               re.I),  # "# AuthorName"
    # Fix CLEANER-C: date lines from Royal Road author section
    re.compile(r"^-\s+\*\*\s+\w{3}",              re.I),  # "- ** Monday, ..."
    re.compile(r"^\w+\s+Lakes?\s+sect\s*$",        re.I),  # Location tags like "Thousand Lakes sect"
]


def _strip_author_bio(text: str) -> str:
    lines  = text.splitlines()
    n      = len(lines)
    cutoff = max(5, int(n * 0.55))  # Lowered từ 0.60 → 0.55 để catch sớm hơn

    # Step 1: Tìm bio marker đầu tiên từ dưới lên
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

    # Step 2: Greedy upward scan — tìm START của noise block
    # Dừng khi gặp 2+ dòng prose thật liên tiếp (>= 5 words, không phải bio marker)
    cut_pos          = first_marker
    consecutive_prose = 0
    scan_limit        = max(cutoff - 1, first_marker - 50)  # max 50 dòng lên trên

    for i in range(first_marker - 1, scan_limit, -1):
        stripped = lines[i].strip()

        if not stripped:
            # Empty line → không phải prose, không phải noise → giữ cut_pos hiện tại
            continue

        is_bio_marker = any(p.search(stripped) for p in _BIO_RE)
        # Short line (≤ 4 words) = likely nav/label/stat line, NOT prose
        is_short_noise = len(stripped.split()) <= 4 and not re.search(r"[.!?]$", stripped)

        if is_bio_marker or is_short_noise:
            cut_pos           = i
            consecutive_prose = 0
        else:
            consecutive_prose += 1
            if consecutive_prose >= 2:
                # 2 dòng prose thật liên tiếp → đây là content thật, dừng
                break

    candidate = "\n".join(lines[:cut_pos])
    if len(candidate.strip()) >= _MIN_REMAINING:
        return candidate
    return text


# ── Main entry point ──────────────────────────────────────────────────────────

def clean_extracted_content(text: str) -> str:
    """
    Apply tất cả 5 cleaning passes theo thứ tự.

    Pass order:
        1. _strip_comment_section  (từ 30% trở xuống)
        2. _strip_settings_panel   (bất kỳ vị trí)
        3. _strip_postfix_section  (từ 35% trở xuống) [NEW]
        4. _strip_metadata_header  (25 dòng đầu)
        5. _strip_author_bio       (từ 55% trở xuống)

    Conservative: không bao giờ return ít hơn 40% original content.
    """
    if not text or len(text.strip()) < _MIN_REMAINING:
        return text

    original_len = len(text.strip())
    result       = text

    result = _strip_comment_section(result)    # Pass 1
    result = _strip_settings_panel(result)     # Pass 2
    result = _strip_postfix_section(result)    # Pass 3 (NEW)
    result = _strip_metadata_header(result)    # Pass 4
    result = _strip_author_bio(result)         # Pass 5

    cleaned_len = len(result.strip())

    # Safety: nếu strip > 60% → return original
    if cleaned_len < original_len * (1 - _MAX_STRIP_RATIO):
        return text

    return result.strip() if result.strip() else text