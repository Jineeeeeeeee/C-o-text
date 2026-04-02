"""
utils/ads_filter.py — Lọc watermark/ads từ nội dung chương truyện.

Simplified từ phiên bản cũ:
  - Không còn build_ai_context_block (AI scan được tích hợp vào Learning Phase)
  - Inject keywords từ profile khi khởi động
  - Có thể add keywords mới trong runtime
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import TYPE_CHECKING

from config import ADS_DB_FILE

if TYPE_CHECKING:
    from utils.types import SiteProfile

logger = logging.getLogger(__name__)

_MIN_LINE_LEN = 15

_SEED_KEYWORDS: list[str] = [
    "stolen content", "stolen from", "this content is stolen",
    "this chapter is stolen", "has been taken without permission",
    "read at royalroad", "read on royalroad", "read the original at",
    "find this and other great novels", "please support the author",
    "support the original", "patreon.com/", "ko-fi.com/",
    "read at scribblehub", "read on scribblehub",
    "original at webnovel", "read on webnovel",
    "keyboard keys to browse between chapters",
    "use left, right keyboard keys",
    "if you find any errors", "translate by", "translation by",
    "mtl by", "machine translated", "chapters are updated daily",
    "visit lightnovelreader", "visit novelfull", "visit wuxiaworld",
    "read latest chapters at", "read advance chapters at",
]

_SEED_PATTERNS_RAW: list[str] = [
    r"^Tip:\s+You can use",
    r"<script[\s>]", r"</script>",
    r"window\.pubfuturetag", r"window\.googletag",
    r"window\.adsbygoogle", r"googletag\.cmd\.push",
    r"pubfuturetag\.push\(",
    r'"unit"\s*:\s*"[^"]+"\s*,\s*"id"\s*:\s*"pf-',
    r"window\.\w+\s*=\s*window\.\w+\s*\|\|\s*\[\]",
]


class AdsFilter:
    """Lọc ads/watermark bằng keyword và regex."""

    def __init__(self) -> None:
        self._keywords: set[str] = {kw.lower() for kw in _SEED_KEYWORDS}
        self._patterns: list[re.Pattern[str]] = []
        for raw in _SEED_PATTERNS_RAW:
            try:
                self._patterns.append(re.compile(raw, re.IGNORECASE))
            except re.error:
                pass

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def load(cls) -> "AdsFilter":
        """Load từ file + seed keywords."""
        instance = cls()
        if not os.path.exists(ADS_DB_FILE):
            return instance
        try:
            with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for kw in data.get("keywords", []):
                if isinstance(kw, str):
                    kw_lower = kw.lower().strip()
                    if kw_lower:
                        instance._keywords.add(kw_lower)
            for pat in data.get("patterns", []):
                if isinstance(pat, str) and pat.strip():
                    try:
                        instance._patterns.append(re.compile(pat.strip(), re.IGNORECASE))
                    except re.error:
                        pass
        except Exception as e:
            logger.warning("[AdsFilter] Load thất bại: %s", e)
        return instance

    def save(self) -> None:
        os.makedirs(os.path.dirname(ADS_DB_FILE) or ".", exist_ok=True)
        tmp = ADS_DB_FILE + ".tmp"
        try:
            data = {
                "keywords": sorted(self._keywords),
                "patterns": [p.pattern for p in self._patterns],
            }
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, ADS_DB_FILE)
        except Exception as e:
            logger.error("[AdsFilter] Lưu thất bại: %s", e)
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

    # ── Inject từ profile ─────────────────────────────────────────────────────

    def inject_from_profile(self, profile: "SiteProfile") -> int:
        """Inject ads_keywords_learned từ profile. Trả về số keyword mới."""
        added = 0
        for kw in profile.get("ads_keywords_learned") or []:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if kw_lower and kw_lower not in self._keywords:
                self._keywords.add(kw_lower)
                added += 1
        return added

    def add_keywords(self, keywords: list[str]) -> int:
        """Thêm keywords mới. Trả về số keyword thực sự mới."""
        added = 0
        for kw in keywords:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if kw_lower and kw_lower not in self._keywords:
                self._keywords.add(kw_lower)
                added += 1
        return added

    # ── Core filtering ────────────────────────────────────────────────────────

    def filter(self, text: str) -> str:
        """Lọc ads khỏi text, gộp blank lines thừa."""
        lines = [ln for ln in text.splitlines() if not self._is_ads(ln)]
        result: list[str] = []
        blanks = 0
        for ln in lines:
            if not ln.strip():
                blanks += 1
                if blanks <= 1:
                    result.append(ln)
            else:
                blanks = 0
                result.append(ln)
        return "\n".join(result)

    def _is_ads(self, line: str) -> bool:
        stripped = line.strip()
        if len(stripped) < _MIN_LINE_LEN:
            return False
        lower = stripped.lower()
        for kw in self._keywords:
            if kw in lower:
                return True
        for pat in self._patterns:
            if pat.search(stripped):
                return True
        return False

    @property
    def stats(self) -> str:
        return f"{len(self._keywords)}kw/{len(self._patterns)}pat"