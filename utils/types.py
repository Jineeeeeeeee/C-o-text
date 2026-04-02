"""
utils/types.py — TypedDict definitions cho toàn bộ project.

SiteProfile là schema trung tâm — chứa tất cả thông tin học được từ
Thorough Learning Mode và được dùng trong Full Scrape Mode.
"""
from __future__ import annotations
from typing import Optional, TypedDict


# ── Formatting rules ──────────────────────────────────────────────────────────

class SpecialElementRule(TypedDict, total=False):
    """Rule chuyển đổi một loại element đặc biệt sang Markdown."""
    found     : bool
    selectors : list[str]   # CSS selectors: [".well", "div.system-box", "#notice"]
    convert_to: str          # "blockquote" | "code_block" | "italic_note" | "spoiler_tag" | "skip"
    prefix    : str          # Chỉ dùng với convert_to="blockquote", VD: "**System:**"


class FormattingRules(TypedDict, total=False):
    """
    Tất cả quy tắc format được AI học từ Phase 2.
    Được MarkdownFormatter đọc khi chuyển HTML → Markdown.
    """
    # Content structure
    tables            : bool              # Có bảng HTML không?
    bold_italic       : bool              # Giữ **bold** và *italic* không?
    hr_dividers       : bool              # Giữ --- divider không?
    image_alt_text    : bool              # Ghi alt text của ảnh không?

    # Math
    math_support      : bool              # Site có công thức toán không?
    math_format       : Optional[str]     # "latex" | "mathjax" | "plain_unicode"

    # Special elements (RPG/LitRPG)
    system_box        : Optional[SpecialElementRule]   # Status box, skill notification
    hidden_text       : Optional[SpecialElementRule]   # Spoiler, censored
    author_note       : Optional[SpecialElementRule]   # Author's note / TN

    # Ký hiệu đặc biệt quan sát được
    special_symbols   : list[str]         # ["—", "…", "™", "©", "·"]


# ── Site profile ──────────────────────────────────────────────────────────────

class SiteProfile(TypedDict, total=False):
    """
    Profile đầy đủ cho một domain — persist qua các session.

    Được tạo bởi Thorough Learning Mode (5 AI calls).
    Đọc bởi Full Scrape Mode để extract + format không cần AI.
    """
    # Identity
    domain        : str
    last_learned  : str          # ISO datetime của lần học gần nhất
    confidence    : float        # 0.0–1.0, tổng hợp từ AI Call #5

    # Selectors (CSS)
    content_selector : Optional[str]    # Element chứa nội dung truyện
    next_selector    : Optional[str]    # Link/nút sang chương tiếp
    title_selector   : Optional[str]    # Tiêu đề chương
    remove_selectors : list[str]        # Elements cần xóa trước khi extract

    # Navigation
    nav_type             : Optional[str]   # "selector"|"rel_next"|"slug_increment"|"fanfic"
    chapter_url_pattern  : Optional[str]   # Regex Python nhận diện URL chapter
    requires_playwright  : bool

    # Formatting rules (NEW — từ Phase 2)
    formatting_rules  : FormattingRules

    # Learned data
    ads_keywords_learned : list[str]    # Keywords watermark học được
    learned_chapters     : list[int]    # [1, 2, 3, 4, 5]
    sample_urls          : list[str]    # URLs đã dùng để học


# ── Progress ──────────────────────────────────────────────────────────────────

class ProgressDict(TypedDict, total=False):
    # Scraping state
    current_url      : Optional[str]
    chapter_count    : int
    story_title      : Optional[str]
    all_visited_urls : list[str]
    fingerprints     : list[str]

    # Story ID guard
    story_id        : Optional[str]
    story_id_regex  : Optional[str]
    story_id_locked : bool

    # Completion
    completed        : bool
    completed_at_url : Optional[str]

    # Learning phase flag
    learning_done : bool
    start_url     : str    # URL gốc từ links.txt (dùng để re-scrape từ đầu)


# ── AI result types ───────────────────────────────────────────────────────────

class AiClassifyResult(TypedDict, total=False):
    page_type         : str
    next_url          : Optional[str]
    first_chapter_url : Optional[str]


class AiInitialProfile(TypedDict, total=False):
    """Kết quả từ AI Call #1."""
    content_selector    : Optional[str]
    next_selector       : Optional[str]
    title_selector      : Optional[str]
    remove_selectors    : list[str]
    nav_type            : Optional[str]
    chapter_url_pattern : Optional[str]
    requires_playwright : bool
    notes               : Optional[str]


class AiValidation(TypedDict, total=False):
    """Kết quả từ AI Call #2."""
    content_selector_valid : bool
    content_selector_fix   : Optional[str]
    next_selector_valid    : bool
    next_selector_fix      : Optional[str]
    title_selector_valid   : bool
    title_selector_fix     : Optional[str]
    notes                  : Optional[str]


class AiSpecialContent(TypedDict, total=False):
    """Kết quả từ AI Call #3."""
    has_tables      : bool
    has_math        : bool
    math_format     : Optional[str]
    math_evidence   : list[str]
    special_symbols : list[str]
    notes           : Optional[str]


class AiFormattingAnalysis(TypedDict, total=False):
    """Kết quả từ AI Call #4."""
    system_box          : dict
    hidden_text         : dict
    author_note         : dict
    bold_italic         : bool
    hr_dividers         : bool
    image_alt_text      : bool
    notes               : Optional[str]


class AiFinalCrosscheck(TypedDict, total=False):
    """Kết quả từ AI Call #5."""
    confidence              : float
    content_selector_final  : Optional[str]
    next_selector_final     : Optional[str]
    title_selector_final    : Optional[str]
    remove_selectors_final  : list[str]
    ads_keywords            : list[str]
    notes                   : Optional[str]