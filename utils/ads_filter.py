"""
utils/ads_filter.py — Lọc watermark/ads từ nội dung chương truyện.

Thay đổi so với phiên bản cũ:
  - Keywords tách thành global + per-domain (không dùng chung nữa)
  - JSON format mới: {"global": {...}, "domains": {"royalroad.com": {...}}}
  - Backward compatible với format cũ (flat list → tự migrate sang global)
  - AdsFilter.load(domain=...) để load đúng domain bucket
  - Ưu tiên CSS hidden-element removal từ html_filter.py, keyword chỉ là lưới cuối
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

# ── Global keywords (thực sự universal — xuất hiện trên MỌI site vi phạm) ────
_SEED_GLOBAL_KEYWORDS: list[str] = [
    "stolen content", "stolen from", "this content is stolen",
    "this chapter is stolen", "has been taken without permission",
    "please support the author", "support the original",
    "patreon.com/", "ko-fi.com/",
    "translate by", "translation by",
    "mtl by", "machine translated",
    "if you find any errors",
    "read latest chapters at", "read advance chapters at",
    "chapters are updated daily",
]

# ── Per-domain seed keywords (chỉ dùng khi scrape đúng domain đó) ────────────
_SEED_DOMAIN_KEYWORDS: dict[str, list[str]] = {
    "royalroad.com": [
        "read at royalroad", "read on royalroad",
        "find this and other great novels",
        "keyboard keys to browse between chapters",
        "use left, right keyboard keys",
    ],
    "scribblehub.com": [
        "read at scribblehub", "read on scribblehub",
        "sh-notice",
    ],
    "webnovel.com": [
        "original at webnovel", "read on webnovel",
    ],
    "lightnovelreader.me": [
        "visit lightnovelreader",
    ],
    "novelfull.com": [
        "visit novelfull",
    ],
    "wuxiaworld.com": [
        "visit wuxiaworld",
    ],
    "fanfiction.net": [
        "story text placeholder",
    ],
}

# ── Regex patterns (global — chặn JS/ad injection code) ──────────────────────
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
    """
    Lọc ads/watermark bằng keyword và regex.

    Kiến trúc:
    - _global_*  : áp dụng với mọi domain
    - _domain_*  : chỉ áp dụng với self._domain
    - Khi filter(): kiểm tra global trước, domain sau
    """

    def __init__(self, domain: str | None = None) -> None:
        self._domain = domain

        # Global
        self._global_keywords: set[str] = {kw.lower() for kw in _SEED_GLOBAL_KEYWORDS}
        self._global_patterns: list[re.Pattern[str]] = []
        for raw in _SEED_PATTERNS_RAW:
            try:
                self._global_patterns.append(re.compile(raw, re.IGNORECASE))
            except re.error:
                pass

        # Domain-specific
        self._domain_keywords: set[str] = set()
        self._domain_patterns: list[re.Pattern[str]] = []
        if domain:
            for key, kws in _SEED_DOMAIN_KEYWORDS.items():
                if key in domain:
                    for kw in kws:
                        self._domain_keywords.add(kw.lower())

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def load(cls, domain: str | None = None) -> "AdsFilter":
        """
        Load từ file + seed keywords.
        Tự migrate từ format cũ (flat keywords/patterns) sang format mới.
        """
        instance = cls(domain)
        if not os.path.exists(ADS_DB_FILE):
            return instance

        try:
            with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if "global" in data:
                # ── New format ─────────────────────────────────────────────
                _load_bucket(data["global"], instance._global_keywords, instance._global_patterns)

                if domain and "domains" in data:
                    for d_key, d_bucket in data["domains"].items():
                        # Match linh hoạt: "royalroad.com" khớp với "www.royalroad.com"
                        if d_key in domain or domain in d_key:
                            _load_bucket(
                                d_bucket,
                                instance._domain_keywords,
                                instance._domain_patterns,
                            )
            else:
                # ── Old flat format → migrate vào global ──────────────────
                logger.info("[AdsFilter] Migrating old format → global bucket")
                _load_bucket(data, instance._global_keywords, instance._global_patterns)

        except Exception as e:
            logger.warning("[AdsFilter] Load thất bại: %s", e)

        return instance

    def save(self) -> None:
        """
        Lưu tất cả keywords/patterns xuống file.
        Đọc file hiện tại trước để merge (không overwrite domain khác).
        """
        # Đọc existing data
        existing: dict = {"global": {"keywords": [], "patterns": []}, "domains": {}}
        if os.path.exists(ADS_DB_FILE):
            try:
                with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if "global" in raw:
                    existing = raw
                else:
                    # Migrate old format
                    existing["global"]["keywords"] = raw.get("keywords", [])
                    existing["global"]["patterns"] = raw.get("patterns", [])
            except Exception:
                pass

        # Ensure structure
        existing.setdefault("global", {"keywords": [], "patterns": []})
        existing.setdefault("domains", {})

        # Merge global
        g_kws = set(existing["global"].get("keywords", []))
        g_kws.update(self._global_keywords)
        existing["global"]["keywords"] = sorted(g_kws)

        g_pats = set(existing["global"].get("patterns", []))
        g_pats.update(p.pattern for p in self._global_patterns)
        existing["global"]["patterns"] = sorted(g_pats)

        # Merge domain
        if self._domain and (self._domain_keywords or self._domain_patterns):
            if self._domain not in existing["domains"]:
                existing["domains"][self._domain] = {"keywords": [], "patterns": []}
            d = existing["domains"][self._domain]
            d_kws = set(d.get("keywords", []))
            d_kws.update(self._domain_keywords)
            d["keywords"] = sorted(d_kws)
            d_pats = set(d.get("patterns", []))
            d_pats.update(p.pattern for p in self._domain_patterns)
            d["patterns"] = sorted(d_pats)

        # Atomic write
        os.makedirs(os.path.dirname(ADS_DB_FILE) or ".", exist_ok=True)
        tmp = ADS_DB_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
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
        """
        Inject ads_keywords_learned từ profile → domain bucket (không global).
        Trả về số keyword thực sự mới.
        """
        added = 0
        for kw in profile.get("ads_keywords_learned") or []:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if (
                kw_lower
                and kw_lower not in self._global_keywords
                and kw_lower not in self._domain_keywords
            ):
                self._domain_keywords.add(kw_lower)
                added += 1
        return added

    def add_keywords(self, keywords: list[str], to_domain: bool = True) -> int:
        """
        Thêm keywords mới.
        to_domain=True (default): thêm vào domain bucket nếu có domain, else global.
        """
        added = 0
        use_domain = to_domain and bool(self._domain)
        for kw in keywords:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if not kw_lower:
                continue
            if kw_lower in self._global_keywords or kw_lower in self._domain_keywords:
                continue
            if use_domain:
                self._domain_keywords.add(kw_lower)
            else:
                self._global_keywords.add(kw_lower)
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
        for kw in self._global_keywords:
            if kw in lower:
                return True
        for kw in self._domain_keywords:
            if kw in lower:
                return True
        for pat in self._global_patterns:
            if pat.search(stripped):
                return True
        for pat in self._domain_patterns:
            if pat.search(stripped):
                return True
        return False

    @property
    def stats(self) -> str:
        total_kw  = len(self._global_keywords) + len(self._domain_keywords)
        total_pat = len(self._global_patterns)  + len(self._domain_patterns)
        if self._domain:
            return (
                f"{total_kw}kw "
                f"({len(self._global_keywords)}g+{len(self._domain_keywords)}local)"
                f"/{total_pat}pat"
            )
        return f"{total_kw}kw/{total_pat}pat"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_bucket(
    bucket: dict,
    kw_set: set[str],
    pat_list: list[re.Pattern[str]],
) -> None:
    """Load keywords + patterns từ một bucket dict vào các set/list đã có."""
    for kw in bucket.get("keywords", []):
        if isinstance(kw, str):
            kw_lower = kw.lower().strip()
            if kw_lower:
                kw_set.add(kw_lower)
    for pat in bucket.get("patterns", []):
        if isinstance(pat, str) and pat.strip():
            try:
                pat_list.append(re.compile(pat.strip(), re.IGNORECASE))
            except re.error:
                pass