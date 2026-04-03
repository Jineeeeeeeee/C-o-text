"""
utils/ads_filter.py — v4: Lọc watermark/ads + session logging + AI verify + blacklist generic keywords.

Thay đổi so với v3:
  - add_keywords(): thêm blacklist từ generic/short (search, log in, read, find, chapter, story, etc.)
  - Tránh học keyword < 8 ký tự hoặc quá rõ ràng là từ story
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config import ADS_DB_FILE

if TYPE_CHECKING:
    from utils.types import SiteProfile

logger = logging.getLogger(__name__)

_MIN_LINE_LEN   = 15
_ADS_REVIEW_DIR = os.path.join(os.path.dirname(ADS_DB_FILE), "ads_review")

# ── Global keywords ───────────────────────────────────────────────────────────
_SEED_GLOBAL_KEYWORDS: list[str] = [
    "stolen content", "stolen from", "this content is stolen",
    "this chapter is stolen", "has been taken without permission",
    "please support the author", "support the original",
    "patreon.com/", "ko-fi.com/",
    "translation by", "mtl by", "machine translated",
    "if you find any errors",
    "read latest chapters at", "read advance chapters at",
    "chapters are updated daily",
]

# ── Per-domain seed keywords ──────────────────────────────────────────────────
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
    "lightnovelreader.me": ["visit lightnovelreader"],
    "novelfull.com"      : ["visit novelfull"],
    "wuxiaworld.com"     : ["visit wuxiaworld"],
    "fanfiction.net"     : ["story text placeholder"],
}

# ── Regex patterns (global) ───────────────────────────────────────────────────
_SEED_PATTERNS_RAW: list[str] = [
    r"^Tip:\s+You can use",
    r"<script[\s>]", r"</script>",
    r"window\.pubfuturetag", r"window\.googletag",
    r"window\.adsbygoogle", r"googletag\.cmd\.push",
    r"pubfuturetag\.push\(",
    r'"unit"\s*:\s*"[^"]+"\s*,\s*"id"\s*:\s*"pf-',
    r"window\.\w+\s*=\s*window\.\w+\s*\|\|\s*\[\]",
]

# FIX: Blacklist từ generic/short để tránh học keywords sai
_GENERIC_KEYWORD_BLACKLIST: frozenset[str] = frozenset({
    # Quá short hoặc quá generic
    "search", "log in", "login", "read", "find", "chapter", "story",
    "novel", "series", "book", "text", "content", "page", "link", "click",
    "here", "site", "web", "online", "free",
    
    # Từ site được loại trừ (sẽ match cả nội dung)
    "royal road", "royalroad", "fanfiction", "wattpad", "webnovel",
    "scribble", "archive", "ao3",
    
    # Tiêu đề story/tên nhân vật hay bị nhầm
    "the primal hunter", "monster cultivator", "system", "bloodline",
    "realm", "cultivation", "dungeon", "quest", "skill", "class",
})


class AdsFilter:
    """
    Lọc ads/watermark bằng keyword và regex.

    Kiến trúc:
      - _global_*     : áp dụng với mọi domain
      - _domain_*     : chỉ áp dụng với self._domain
      - _session_log  : log mọi dòng bị filter trong phiên hiện tại
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

        # Session log (reset mỗi phiên scrape)
        self._session_log: list[dict] = []

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def load(cls, domain: str | None = None) -> "AdsFilter":
        """Load từ file + seed keywords. Tự migrate format cũ."""
        instance = cls(domain)
        if not os.path.exists(ADS_DB_FILE):
            return instance
        try:
            with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "global" in data:
                _load_bucket(data["global"], instance._global_keywords, instance._global_patterns)
                if domain and "domains" in data:
                    for d_key, d_bucket in data["domains"].items():
                        if d_key in domain or domain in d_key:
                            _load_bucket(
                                d_bucket,
                                instance._domain_keywords,
                                instance._domain_patterns,
                            )
            else:
                logger.info("[AdsFilter] Migrating old format → global bucket")
                _load_bucket(data, instance._global_keywords, instance._global_patterns)
        except Exception as e:
            logger.warning("[AdsFilter] Load thất bại: %s", e)
        return instance

    def save(self) -> None:
        """Lưu tất cả keywords/patterns xuống file (merge, không overwrite domain khác)."""
        existing: dict = {"global": {"keywords": [], "patterns": []}, "domains": {}}
        if os.path.exists(ADS_DB_FILE):
            try:
                with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if "global" in raw:
                    existing = raw
                else:
                    existing["global"]["keywords"] = raw.get("keywords", [])
                    existing["global"]["patterns"] = raw.get("patterns", [])
            except Exception:
                pass

        existing.setdefault("global", {"keywords": [], "patterns": []})
        existing.setdefault("domains", {})

        g_kws = set(existing["global"].get("keywords", []))
        g_kws.update(self._global_keywords)
        existing["global"]["keywords"] = sorted(g_kws)

        g_pats = set(existing["global"].get("patterns", []))
        g_pats.update(p.pattern for p in self._global_patterns)
        existing["global"]["patterns"] = sorted(g_pats)

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

    # ── Inject từ profile ─────────────────────────────────────────────────

    def inject_from_profile(self, profile: "SiteProfile") -> int:
        """Inject ads_keywords_learned từ profile → domain bucket."""
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
        Thêm keywords mới vào domain bucket (default) hoặc global.
        
        FIX: Bỏ qua keyword nếu:
          1. Trong blacklist generic (search, log in, etc.)
          2. Quá short (< 8 ký tự)
          3. Đã trong global hoặc domain bucket
        """
        added = 0
        use_domain = to_domain and bool(self._domain)
        
        for kw in keywords:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if not kw_lower:
                continue
            
            # FIX: Skip blacklist generic keywords
            if kw_lower in _GENERIC_KEYWORD_BLACKLIST:
                logger.debug(f"[AdsFilter] Skip blacklist keyword: {kw_lower!r}")
                continue
            
            # Skip quá short
            if len(kw_lower) < 8:
                logger.debug(f"[AdsFilter] Skip too short keyword: {kw_lower!r}")
                continue
            
            # Skip nếu đã tồn tại
            if kw_lower in self._global_keywords or kw_lower in self._domain_keywords:
                continue
            
            if use_domain:
                self._domain_keywords.add(kw_lower)
            else:
                self._global_keywords.add(kw_lower)
            added += 1
        
        return added

    # ── Core filtering ────────────────────────────────────────────────────

    def filter(self, text: str, chapter_url: str = "") -> str:
        """
        Lọc ads khỏi text, gộp blank lines thừa.
        Ghi log mọi dòng bị filter kèm chapter_url để audit sau.
        """
        lines = text.splitlines()
        kept: list[str] = []

        for ln in lines:
            stripped = ln.strip()
            # Chỉ check + log các dòng đủ dài
            if len(stripped) >= _MIN_LINE_LEN and self._is_ads(ln):
                self._session_log.append({
                    "line"        : stripped,
                    "chapter_url" : chapter_url,
                })
            else:
                kept.append(ln)

        result: list[str] = []
        blanks = 0
        for ln in kept:
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

    # ── Session log API ───────────────────────────────────────────────────

    def get_session_summary(self) -> dict[str, dict]:
        """
        Aggregate session log thành: line → {count, urls}.
        Dùng để biết dòng nào bị filter nhiều nhất và ở chương nào.
        """
        summary: dict[str, dict] = {}
        for entry in self._session_log:
            line = entry["line"]
            if line not in summary:
                summary[line] = {"count": 0, "urls": []}
            summary[line]["count"] += 1
            url = entry.get("chapter_url", "")
            if url and url not in summary[line]["urls"]:
                summary[line]["urls"].append(url)
        return summary

    def clear_session_log(self) -> None:
        """Xóa log phiên hiện tại (dùng khi bắt đầu truyện mới)."""
        self._session_log.clear()

    def get_unknown_candidates(
        self,
        min_count: int = 2,
        max_results: int = 20,
    ) -> list[str]:
        """
        Trả về top N dòng bị filter mà CHƯA được cover bởi keyword đã biết.
        Dùng để gửi AI xác nhận xem có phải ads thật không.

        Fix #1: Dùng substring check (giống _is_ads) thay vì equality check.
        Lý do: _is_ads lọc bằng `kw in lower` (substring), nên một dòng như
        "This content is stolen from royalroad" bị filter vì chứa "stolen content",
        nhưng equality check `lower in self._global_keywords` lại không khớp
        → dòng đó bị đưa vào candidates AI một cách thừa thãi.

        min_count: chỉ lấy dòng xuất hiện >= N lần (tránh one-off false positive)
        """
        summary = self.get_session_summary()
        candidates: list[str] = []
        for line, info in sorted(summary.items(), key=lambda x: -x[1]["count"]):
            if info["count"] < min_count:
                continue
            lower = line.lower()
            # Substring check — khớp với logic trong _is_ads()
            already_known = (
                any(kw in lower for kw in self._global_keywords)
                or any(kw in lower for kw in self._domain_keywords)
            )
            if already_known:
                continue
            candidates.append(line)
            if len(candidates) >= max_results:
                break
        return candidates

    def apply_verified(self, confirmed_lines: list[str]) -> int:
        """Thêm AI-confirmed lines vào domain keyword bucket."""
        return self.add_keywords(confirmed_lines, to_domain=True)

    # ── Persistent review file ────────────────────────────────────────────

    def save_pending_review(
        self,
        domain_slug: str,
        verified_results: dict[str, bool] | None = None,
    ) -> str | None:
        """
        Merge session log vào file review bền vững.
        Format: data/ads_review/<domain_slug>_pending.json
        """
        summary = self.get_session_summary()
        if not summary:
            return None

        os.makedirs(_ADS_REVIEW_DIR, exist_ok=True)
        review_path = os.path.join(_ADS_REVIEW_DIR, f"{domain_slug}_pending.json")

        existing: dict[str, dict] = {}
        if os.path.exists(review_path):
            try:
                with open(review_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for entry in data.get("entries", []):
                    existing[entry["line"]] = entry
            except Exception:
                pass

        now_iso = datetime.now(timezone.utc).isoformat()
        for line, info in summary.items():
            verified = (verified_results or {}).get(line, None)
            if line in existing:
                existing[line]["count"] += info["count"]
                for url in info["urls"]:
                    if url not in existing[line].get("story_urls", []):
                        existing[line].setdefault("story_urls", []).append(url)
                if verified is not None:
                    existing[line]["ai_verified"] = verified
                    existing[line]["verified_at"] = now_iso
            else:
                entry: dict = {
                    "line"        : line,
                    "count"       : info["count"],
                    "story_urls"  : info["urls"],
                    "ai_verified" : verified,
                }
                if verified is not None:
                    entry["verified_at"] = now_iso
                existing[line] = entry

        output = {
            "domain"      : self._domain,
            "last_updated": now_iso,
            "entries"     : sorted(
                existing.values(),
                key=lambda x: x["count"],
                reverse=True,
            ),
        }
        tmp = review_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)
            os.replace(tmp, review_path)
            return review_path
        except Exception as e:
            logger.error("[AdsFilter] save_pending_review thất bại: %s", e)
            try:
                os.remove(tmp)
            except Exception:
                pass
            return None

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