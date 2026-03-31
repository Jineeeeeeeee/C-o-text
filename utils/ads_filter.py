# utils/ads_filter.py
"""
utils/ads_filter.py — Lightweight ads/watermark filter cho plain-text content.

Vai trò trong pipeline:
  html_filter.py  → xóa hidden DOM element (CSS-hidden watermark)
  ads_filter.py   → xóa dòng văn bản ads trong nội dung đã extract
                    (plain-text watermark không bị ẩn bởi CSS)

Ví dụ ads dạng plain-text (html_filter.py bỏ qua hoàn toàn):
  "If you come across this story on Amazon, it has been stolen."
  "Read the original at royalroad.com"
  "This content was taken from webnovel without permission."

Thiết kế:
  - 1 instance per novel task (không share giữa các truyện)
  - Không thread-safe — chạy trong 1 asyncio task duy nhất, không cần lock
  - Stateful: keyword/pattern set tăng dần theo thời gian qua AI learning

API công khai:
  filter_content(text)            → str      xóa dòng chứa ads
  build_ai_context_block(text)    → str|None block text gửi ai_detect_ads_content
  update_from_ai_result(raw_json) → int      học pattern mới, trả về số đã thêm
"""
from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)

# ── Seed keywords ─────────────────────────────────────────────────────────────
# Cụm từ watermark phổ biến từ aggregator / piracy sites.
# Lowercase — so sánh với line.lower() để case-insensitive.
# Bổ sung khi gặp thực tế: thêm vào list này rồi commit.
_SEED_KEYWORDS: list[str] = [
    # Generic stolen-content notice
    "stolen content",
    "stolen from",
    "this content is stolen",
    "this chapter is stolen",
    "this chapter was stolen",
    "this work has been stolen",
    "if you come across this story",
    "if you find this content",
    "this story has been stolen",
    "has been taken without permission",
    "taken without permission",

    # Site-specific "read at original"
    "read at royalroad",
    "read on royalroad",
    "read the original at",
    "read the original on",
    "original source",
    "find this and other great novels",
    "check out the original",
    "visit the original",

    # Support author
    "please support the author",
    "support the original",
    "support the original author",

    # "More at" aggregator links
    "for more, visit",
    "more chapters at",
    "read more at",

    # Monetization / donation links (thường là inline text)
    "patreon.com/",
    "ko-fi.com/",
    "buymeacoffee.com/",

    # ScribbleHub / Webnovel specific
    "read at scribblehub",
    "read on scribblehub",
    "original at webnovel",
    "read on webnovel",

    # Wattpad
    "read on wattpad",
    "find this story on wattpad",

    # Amazon / Kindle re-post notice
    "if you encounter this story on amazon",
    "encounter this story on amazon",
    "found on amazon, report it",
]

# Dòng rất ngắn sau strip thường là fragment ads hoặc separator rác
# Nhưng không filter hoàn toàn vì có thể là dòng trống hợp lệ
_MIN_SUSPICIOUS_LINE_LEN = 15

# Số context dòng trước/sau dòng nghi ngờ gửi lên AI
_CONTEXT_WINDOW = 10

# Tối đa N block nghi ngờ gửi lên AI trong 1 lần scan
# Giới hạn để tránh prompt quá dài → tốn token
_MAX_CONTEXT_BLOCKS = 5


# ── SimpleAdsFilter ───────────────────────────────────────────────────────────

class SimpleAdsFilter:
    """
    Filter watermark/ads nhẹ cho plain-text nội dung chương.

    Không thread-safe — thiết kế để dùng trong 1 asyncio task duy nhất.
    Mỗi run_novel_task tạo 1 instance riêng biệt.
    """

    def __init__(self) -> None:
        # Set keyword lowercase — lookup O(1) với `in`
        self._keywords: set[str] = {kw.lower() for kw in _SEED_KEYWORDS}
        # List compiled regex — thêm dần qua AI learning
        self._patterns: list[re.Pattern[str]] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def filter_content(self, text: str) -> str:
        """
        Xóa các dòng bị detect là ads/watermark khỏi nội dung chương.

        Thuật toán:
          1. Tách theo dòng
          2. Giữ lại dòng không phải ads
          3. Gộp lại, normalize dòng trắng (tối đa 2 liên tiếp)

        Không xóa dòng trắng hợp lệ → giữ nguyên cấu trúc đoạn văn.
        """
        lines  = text.splitlines()
        kept   = [line for line in lines if not self._is_ads_line(line)]

        # Normalize: sau khi xóa dòng ads, 2 đoạn văn có thể để lại
        # 2 dòng trắng liên tiếp → chuẩn markdown chỉ cần 1 dòng trắng.
        # Giới hạn tối đa 1 blank line liên tiếp.
        result: list[str] = []
        blank_count = 0
        for line in kept:
            if not line.strip():
                blank_count += 1
                if blank_count <= 1:
                    result.append(line)
            else:
                blank_count = 0
                result.append(line)

        return "\n".join(result)

    def build_ai_context_block(self, text: str) -> str | None:
        """
        Tạo block text có định dạng để gửi lên ai_detect_ads_content.

        Với mỗi dòng nghi ngờ, chèn _CONTEXT_WINDOW dòng trước/sau
        và đánh dấu dòng nghi ngờ bằng >>> <<<.

        Trả về None nếu không tìm thấy dòng nghi ngờ nào — tránh
        gọi AI không cần thiết khi nội dung sạch.

        Ví dụ output:
          Line of story text
          Line of story text
          >>> If you come across this story on Amazon, report it. <<<
          Line of story text
          ...
          ---
          (block tiếp theo nếu có)
        """
        lines = text.splitlines()
        suspicious_indices = [
            i for i, line in enumerate(lines)
            if self._is_ads_line(line)
        ]

        if not suspicious_indices:
            return None

        blocks: list[str] = []
        for idx in suspicious_indices[:_MAX_CONTEXT_BLOCKS]:
            start = max(0, idx - _CONTEXT_WINDOW)
            end   = min(len(lines), idx + _CONTEXT_WINDOW + 1)

            context_lines: list[str] = []
            for i in range(start, end):
                if i == idx:
                    context_lines.append(f">>> {lines[i]} <<<")
                else:
                    context_lines.append(lines[i])

            blocks.append("\n".join(context_lines))

        if not blocks:
            return None

        return "\n\n---\n\n".join(blocks)

    def update_from_ai_result(self, raw_json: str) -> int:
        """
        Parse kết quả JSON từ ai_detect_ads_content và cập nhật filter.

        Expected JSON format:
          {
            "found": true,
            "keywords": ["short phrase", ...],
            "patterns": ["python regex", ...],
            "example_lines": ["exact text", ...]
          }

        Bỏ qua nếu "found" = false hoặc JSON lỗi.
        Bỏ qua pattern nếu regex compile thất bại (AI đôi khi viết sai).

        Returns:
          int: Số keyword + pattern mới được thêm vào (để caller log).
        """
        if not raw_json:
            return 0

        try:
            data = json.loads(raw_json.strip())
        except (json.JSONDecodeError, AttributeError, ValueError):
            logger.debug("[AdsFilter] JSON parse thất bại từ AI response")
            return 0

        if not isinstance(data, dict) or not data.get("found"):
            return 0

        added = 0

        # Học keyword mới
        for kw in data.get("keywords", []):
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if kw_lower and kw_lower not in self._keywords:
                self._keywords.add(kw_lower)
                added += 1
                logger.debug("[AdsFilter] Keyword mới: %r", kw_lower)

        # Học regex pattern mới
        for pat_str in data.get("patterns", []):
            if not isinstance(pat_str, str) or not pat_str.strip():
                continue
            try:
                compiled = re.compile(pat_str.strip(), re.IGNORECASE)
                self._patterns.append(compiled)
                added += 1
                logger.debug("[AdsFilter] Pattern mới: %r", pat_str)
            except re.error as e:
                # AI đôi khi viết regex không hợp lệ — bỏ qua, không crash
                logger.debug("[AdsFilter] Regex lỗi (bỏ qua): %r — %s", pat_str, e)

        return added

    @property
    def keyword_count(self) -> int:
        """Số keyword hiện tại — dùng để debug/log."""
        return len(self._keywords)

    @property
    def pattern_count(self) -> int:
        """Số regex pattern hiện tại — dùng để debug/log."""
        return len(self._patterns)

    # ── Private ───────────────────────────────────────────────────────────────

    def _is_ads_line(self, line: str) -> bool:
        """
        Kiểm tra một dòng có phải ads/watermark không.

        Dòng trống hoặc quá ngắn → không check (tránh false positive).
        Check keyword trước (O(1) per keyword) vì thường đủ rồi.
        Check regex sau (O(len(line)) per pattern) chỉ khi cần.
        """
        stripped = line.strip()

        # Bỏ qua dòng trống hoặc quá ngắn
        if len(stripped) < _MIN_SUSPICIOUS_LINE_LEN:
            return False

        lower = stripped.lower()

        # Keyword check — O(K) với K = số keyword
        for kw in self._keywords:
            if kw in lower:
                return True

        # Regex check — O(P * len(line)) với P = số pattern
        for pat in self._patterns:
            if pat.search(stripped):
                return True

        return False