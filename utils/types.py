# utils/types.py
"""
utils/types.py — TypedDict definitions cho toàn bộ project.

Thay thế raw `dict` để:
  - IDE auto-complete và phát hiện typo ngay lập tức
  - mypy / pyright có thể type-check toàn bộ data flow
  - Dễ thêm field mới mà không sợ quên cập nhật các nơi dùng
"""
from __future__ import annotations

from typing import Optional, TypedDict


# ── Progress ──────────────────────────────────────────────────────────────────

class ProgressDict(TypedDict, total=False):
    """
    Cấu trúc file progress JSON — lưu trạng thái cào của mỗi truyện.

    Dùng `total=False` vì dict này đến từ JSON (có thể thiếu key khi
    file cũ chưa có field mới), và `_sync_load_progress` đã backfill
    bằng `make_default_progress()`.
    """
    current_url:       Optional[str]
    chapter_count:     int
    story_title:       Optional[str]
    all_visited_urls:  list[str]
    fingerprints:      list[str]
    collected_urls:    list[str]
    story_id:          Optional[str]
    story_id_regex:    Optional[str]
    story_id_locked:   bool
    story_id_attempts: int
    completed:         bool
    completed_at_url:  Optional[str]
    last_scraped_url:  Optional[str]
    last_title:        Optional[str]


# ── Site profile ──────────────────────────────────────────────────────────────

class SiteProfileDict(TypedDict, total=False):
    """
    CSS selector profile cho một domain — học qua AI (ask_ai_build_profile).
    Dùng `total=False` vì AI có thể trả về null cho một số field.
    """
    next_selector:    Optional[str]
    title_selector:   Optional[str]
    content_selector: Optional[str]


# ── AI results ────────────────────────────────────────────────────────────────

class AiClassifyResult(TypedDict, total=False):
    """Kết quả JSON từ `ai_classify_and_find`."""
    page_type:         str            # "chapter" | "index" | "other"
    next_url:          Optional[str]
    first_chapter_url: Optional[str]


class StoryIdResult(TypedDict, total=False):
    """Kết quả JSON từ `ask_ai_for_story_id`."""
    story_id:       str
    story_id_regex: str