"""
utils/ads_filter.py — v8: Fix false positive story content + smarter auto-approve.

Thay đổi so với v7:
  FIX-B1: get_candidates_by_frequency() — bỏ auto_threshold generic.
           Auto-approve CHỈ cho lines là script/JS injection (100% ads).
           Tất cả candidates còn lại → AI verify.
  FIX-B3: _looks_like_story_content() — nhận diện RPG/LitRPG markers:
           - Bold/italic markdown (**text**, *text*, ***text***)
           - Bracket notation [Skill Name (Rarity)]
           - Arrow/upgrade notation (-->, →)
           - Status field lines (**Field:** [value])
           - Rarity keywords: (Common), (Rare), (Legendary), (Unique), (Ancient)...
  FIX-B1b: _SUSPECT_MIN_FILES tăng từ 5 → 8 để giảm false positives từ
            scan_edges_for_suspects().

Pipeline ads (không thay đổi):
  [Scraping]
    filter()                  → xóa confirmed keywords ngay (seeds + profile)
    scan_edges_for_suspects() → log dòng lạ edges, KHÔNG xóa
    write_markdown()          → save file với dòng lạ còn nguyên

  [Cuối session]
    get_candidates_by_frequency()    → script lines auto-approve, còn lại → AI
    get_new_frequency_suspects()     → candidates từ edge scan
    ai_verify_ads()                  → AI xác nhận tất cả non-script candidates
    post_process_directory(confirmed)→ xóa retroactively từ files
    add_ads_to_profile()             → lưu profile cho lần sau
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config import ADS_DB_FILE

if TYPE_CHECKING:
    from utils.types import SiteProfile

logger = logging.getLogger(__name__)

_MIN_LINE_LEN   = 15
_ADS_REVIEW_DIR = os.path.join(os.path.dirname(ADS_DB_FILE), "ads_review")
_ADS_DB_WRITE_LOCK = threading.Lock()

# Suspect edge scan config
_SUSPECT_SCAN_EDGES = 8    # Quét N dòng đầu và N dòng cuối mỗi chapter
_SUSPECT_MAX_LEN    = 250  # Watermarks hiếm khi dài hơn thế này
_SUSPECT_MIN_FILES  = 8    # FIX-B1b: Tăng từ 5 → 8 để giảm false positives


# ── Global keywords ───────────────────────────────────────────────────────────
_SEED_GLOBAL_KEYWORDS: list[str] = [
    "stolen content", "stolen from", "this content is stolen",
    "this chapter is stolen", "has been taken without permission",
    "please support the author", "support the original",
    "translation by", "mtl by", "machine translated",
    "if you find any errors",
    "read latest chapters at", "read advance chapters at",
    "chapters are updated daily",
    "read at", "read on", "find this novel at",
    "visit to read", "originally published at",
    "patreon.com/", "ko-fi.com/",
    "share to your friends",
    "share this chapter",
    "share this novel",
    "share this story",
    "share on facebook", "share on twitter", "share on reddit",
    "previous chapter", "next chapter",
    "table of contents",
]

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
    "novelfire.net": [
        "share to your friends",
        "share this chapter",
        "novelfire.net",
        "read at novelfire",
    ],
    "lightnovelreader.me": ["visit lightnovelreader"],
    "novelfull.com"      : ["visit novelfull"],
    "wuxiaworld.com"     : ["visit wuxiaworld"],
    "fanfiction.net"     : ["story text placeholder"],
}

_SEED_PATTERNS_RAW: list[str] = [
    r"^Tip:\s+You can use",
    r"<script[\s>]", r"</script>",
    r"window\.pubfuturetag", r"window\.googletag",
    r"window\.adsbygoogle", r"googletag\.cmd\.push",
    r"pubfuturetag\.push\(",
    r'"unit"\s*:\s*"[^"]+"\s*,\s*"id"\s*:\s*"pf-',
    r"window\.\w+\s*=\s*window\.\w+\s*\|\|\s*\[\]",
    r"p[.\s]*a[.\s]*t[.\s]*r[.\s]*e[.\s]*o[.\s]*n",
    r"b[.\s]*o[.\s]*o[.\s]*s[.\s]*t[.\s]*y",
    r"read\s+\d+\s+chapter[s]?\s+ahead",
    r"chapter[s]?\s+ahead\s+(on|at|over)\s+(my\s+)?",
]

_GENERIC_KEYWORD_BLACKLIST: frozenset[str] = frozenset({
    # ── Navigation / UI words — extremely common in story dialogue ────────────
    "next", "previous", "back", "forward", "prev",
    "next chapter", "previous chapter",
    "report", "submit", "save", "load", "reload",

    # ── Generic web/nav words ─────────────────────────────────────────────────
    "search", "log in", "login", "read", "find", "chapter", "story",
    "novel", "series", "book", "text", "content", "page", "link", "click",
    "here", "site", "web", "online", "free", "home", "menu",

    # ── Site names — partial matches dangerous in fantasy story context ───────
    "royal road",           # "royal road to power", "a royal road..."
    "royalroad",
    "fanfiction",           # "fanfiction.net" more specific is OK
    "wattpad", "webnovel",
    "scribble", "archive", "ao3",
    "novel fire",           # "novelfire.net" more specific is OK

    # ── Story-genre common words — LitRPG, xianxia, fantasy ──────────────────
    "the primal hunter", "monster cultivator",
    "system", "bloodline", "realm", "cultivation",
    "dungeon", "quest", "skill", "class",
    "chapter navigation",   # UI label, but too generic as keyword
})

# ── FIX-B3: RPG/Story content markers ────────────────────────────────────────
# Lines matching ANY of these patterns are story content, never ads.
# Checked BEFORE frequency-based candidate selection.

# Script/JS injection — these are 100% ads, safe to auto-approve
_JS_INJECTION_RE = re.compile(
    r"<script[\s>]"
    r"|window\.pubfuturetag"
    r"|window\.googletag"
    r"|window\.adsbygoogle"
    r"|googletag\.cmd\.push"
    r"|pubfuturetag\.push\("
    r"|\"unit\"\s*:\s*\"[^\"]+\"\s*,\s*\"id\"\s*:\s*\"pf-",
    re.IGNORECASE,
)

# RPG/LitRPG system content markers — story content, NOT ads
_RPG_RARITY_RE = re.compile(
    r"\((Common|Uncommon|Rare|Epic|Legendary|Unique|Ancient|Mythic|"
    r"Inferior|Superior|Elite|Boss|Divine|Transcendent)\)",
    re.IGNORECASE,
)

_RPG_BOLD_FIELD_RE = re.compile(
    r"^\*{1,3}[^*].*\*{1,3}\s*$"   # ***text*** or **text** or *text*
    r"|^\*{1,3}\w.*:\*{0,3}"        # **Field:** value
    r"|\*{1,3}[A-Z][^*]+\*{1,3}",  # ***Skill Name Upgraded***
    re.MULTILINE,
)

_RPG_BRACKET_RE = re.compile(
    r"\[[A-Z][^\[\]]{2,60}\]",       # [Skill Name Here]
)

_RPG_UPGRADE_RE = re.compile(
    r"-->"                            # skill upgrade arrow
    r"|\u2192"                        # → unicode arrow
    r"|\bUpgraded\b"
    r"|\bAwakened\b"
    r"|\bTransformed\b",
    re.IGNORECASE,
)

_STORY_LINE_RE = re.compile(
    r'^["""''„]'
    r'|["""''„]$'
    r'|^(The |A |An |He |She |I |It |'
    r'They |We |You |But |And |Or |'
    r'His |Her |My |Our |Their )',
    re.IGNORECASE,
)


def _is_rpg_story_content(line: str) -> bool:
    """
    FIX-B3: Nhận diện RPG/LitRPG system box content.
    Trả về True nếu line là story content (skill box, status screen, v.v.).
    """
    # Rarity keywords — strongest signal
    if _RPG_RARITY_RE.search(line):
        return True
    # Bold/italic markdown wrapping entire line or field label
    if _RPG_BOLD_FIELD_RE.search(line):
        return True
    # Bracket notation [Skill Name ...]
    if _RPG_BRACKET_RE.search(line):
        return True
    # Upgrade/transform arrows
    if _RPG_UPGRADE_RE.search(line):
        return True
    return False


class AdsFilter:
    """
    Lọc ads/watermark bằng keyword và regex.

    v8: Fix B1 (auto-approve chỉ cho JS injection) + Fix B3 (RPG content guard).
    """

    def __init__(self, domain: str | None = None) -> None:
        self._domain = domain

        self._global_keywords: set[str] = {kw.lower() for kw in _SEED_GLOBAL_KEYWORDS}
        self._global_patterns: list[re.Pattern[str]] = []
        for raw in _SEED_PATTERNS_RAW:
            try:
                self._global_patterns.append(re.compile(raw, re.IGNORECASE))
            except re.error:
                pass

        self._domain_keywords: set[str] = set()
        self._domain_patterns: list[re.Pattern[str]] = []
        if domain:
            for key, kws in _SEED_DOMAIN_KEYWORDS.items():
                if key in domain:
                    for kw in kws:
                        self._domain_keywords.add(kw.lower())

        self._session_log: list[dict] = []
        self._freq_counter: dict[str, dict] = {}
        self._notified_ads: set[str] = set()

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def load(cls, domain: str | None = None) -> "AdsFilter":
        """Load từ file + seed keywords."""
        instance = cls(domain)
        if not os.path.exists(ADS_DB_FILE):
            return instance
        try:
            with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                raw = f.read().strip()
            if not raw:
                return instance
            data = json.loads(raw)
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

    # ── Core filtering (real-time) ────────────────────────────────────────

    def filter(self, text: str, chapter_url: str = "") -> str:
        """
        Xóa các dòng khớp với CONFIRMED keywords/patterns.
        Chỉ print thông báo khi gặp ads MỚI (chưa có trong _notified_ads).
        """
        lines = text.splitlines()
        kept: list[str] = []
        new_ads_found: list[str] = []

        for ln in lines:
            stripped = ln.strip()
            if len(stripped) >= _MIN_LINE_LEN and self._is_ads(ln):
                self._session_log.append({
                    "line"       : stripped,
                    "chapter_url": chapter_url,
                })
                key = stripped.lower()
                if key not in self._notified_ads:
                    self._notified_ads.add(key)
                    new_ads_found.append(stripped)
            else:
                kept.append(ln)

        if new_ads_found:
            for ads_line in new_ads_found[:3]:
                short = ads_line[:60] + "…" if len(ads_line) > 60 else ads_line
                print(f"  [Ads] 🆕 NEW: {short!r}", flush=True)
            if len(new_ads_found) > 3:
                print(
                    f"  [Ads] 🆕 +{len(new_ads_found) - 3} more new ads detected",
                    flush=True,
                )

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

    # ── Deferred suspect scanning ─────────────────────────────────────────

    def scan_edges_for_suspects(
        self,
        text          : str,
        chapter_url   : str = "",
        chapter_file  : str = "",
    ) -> None:
        lines = [ln for ln in text.splitlines() if ln.strip()]
        n     = len(lines)
        if n == 0:
            return

        edge = min(_SUSPECT_SCAN_EDGES, n // 2 + 1)
        edge_lines = lines[:edge] + lines[max(0, n - edge):]

        seen_this_chapter: set[str] = set()
        for ln in edge_lines:
            stripped = ln.strip()
            if not (_MIN_LINE_LEN <= len(stripped) <= _SUSPECT_MAX_LEN):
                continue
            if stripped in seen_this_chapter:
                continue
            seen_this_chapter.add(stripped)
            if self._is_ads(stripped):
                continue
            if self._looks_like_story_content(stripped):
                continue
            self._update_freq(stripped, chapter_url, chapter_file)

    def _looks_like_story_content(self, line: str) -> bool:
        """
        FIX-B3: Nhận diện story content để không đưa vào suspect list.
        Bao gồm RPG/LitRPG markers, dialogue, và narrative lines.
        """
        words = line.split()
        # Long lines are almost always story content
        if len(words) > 10:
            return True
        # Story line openers (He/She/I/The/A/But...)
        if _STORY_LINE_RE.match(line):
            return True
        # Dialogue: chứa dấu ngoặc kép → story content
        # Bao gồm cả straight quotes và curly quotes
        if any(c in line for c in ('"', '\u201c', '\u201d', '\u2018', '\u2019')):
            return True
        # Narrative markers: ellipsis hoặc em-dash → thường là story prose
        if '\u2026' in line or '\u2014' in line or '.....' in line:
            return True
        # FIX-B3: RPG system content guard
        if _is_rpg_story_content(line):
            return True
        return False

    def _update_freq(self, line: str, chapter_url: str, chapter_file: str) -> None:
        if line not in self._freq_counter:
            self._freq_counter[line] = {"files": set(), "urls": set()}
        entry = self._freq_counter[line]
        if chapter_file:
            entry["files"].add(chapter_file)
        if chapter_url:
            entry["urls"].add(chapter_url)

    def get_new_frequency_suspects(
        self,
        min_files  : int = _SUSPECT_MIN_FILES,
        max_results: int = 20,
    ) -> list[str]:
        suspects: list[tuple[str, int]] = []
        for line, info in self._freq_counter.items():
            file_count = len(info["files"])
            if file_count < min_files:
                continue
            lower = line.lower()
            if (any(kw in lower for kw in self._global_keywords) or
                    any(kw in lower for kw in self._domain_keywords)):
                continue
            # FIX-B3: Double-check RPG content không lọt vào suspect list
            if _is_rpg_story_content(line):
                logger.debug(
                    "[AdsFilter] Skipping RPG content from suspects: %r", line[:60]
                )
                continue
            suspects.append((line, file_count))
        suspects.sort(key=lambda x: -x[1])
        return [line for line, _ in suspects[:max_results]]

    def get_suspect_file_paths(self, line: str) -> list[str]:
        info = self._freq_counter.get(line)
        if not info:
            return []
        return list(info["files"])

    # ── Retroactive file post-processing ─────────────────────────────────

    @staticmethod
    def post_process_directory(
        confirmed_lines: list[str],
        output_dir     : str,
    ) -> int:
        if not confirmed_lines or not os.path.isdir(output_dir):
            return 0

        confirmed_set = {ln.strip().lower() for ln in confirmed_lines if ln.strip()}
        if not confirmed_set:
            return 0

        total_removed = 0
        for fname in sorted(os.listdir(output_dir)):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(output_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    raw_lines = f.readlines()

                new_lines    : list[str] = []
                file_removed : int       = 0

                for raw_ln in raw_lines:
                    if raw_ln.strip().lower() in confirmed_set:
                        file_removed += 1
                    else:
                        new_lines.append(raw_ln)

                if file_removed > 0:
                    tmp = fpath + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        f.writelines(new_lines)
                    os.replace(tmp, fpath)
                    total_removed += file_removed
                    logger.debug(
                        "[AdsFilter] post_process: -%d lines từ %s",
                        file_removed, fname,
                    )
            except Exception as e:
                logger.warning("[AdsFilter] post_process_directory %s: %s", fname, e)

        return total_removed

    # ── Session log API ───────────────────────────────────────────────────

    def get_session_summary(self) -> dict[str, dict]:
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

    def get_unknown_candidates(
        self,
        min_count  : int = 2,
        max_results: int = 20,
    ) -> list[str]:
        summary    = self.get_session_summary()
        candidates : list[str] = []
        for line, info in sorted(summary.items(), key=lambda x: -x[1]["count"]):
            if info["count"] < min_count:
                continue
            lower = line.lower()
            already_known = (
                any(kw in lower for kw in self._global_keywords) or
                any(kw in lower for kw in self._domain_keywords)
            )
            if already_known:
                continue
            candidates.append(line)
            if len(candidates) >= max_results:
                break
        return candidates

    def get_candidates_by_frequency(
        self,
        auto_threshold: int = 10,   # kept for signature compat, semantics changed
        min_count     : int = 3,
        max_results   : int = 20,
    ) -> tuple[list[str], list[str]]:
        """
        FIX-B1: Tách candidates thành 2 buckets:
          auto_add  — CHỈ script/JS injection lines (100% safe to auto-approve)
          ai_verify — Tất cả candidates còn lại (kể cả count ≥ auto_threshold)

        Lý do: high-frequency không đồng nghĩa với ads — RPG skill names,
        recurring story phrases cũng có thể đạt threshold.
        Script/JS lines thì luôn luôn là ads, không cần AI confirm.
        """
        summary   = self.get_session_summary()
        auto_add  : list[str] = []
        ai_verify : list[str] = []

        for line, info in sorted(summary.items(), key=lambda x: -x[1]["count"]):
            count = info["count"]
            if count < min_count:
                continue

            lower = line.lower()
            already_known = (
                any(kw in lower for kw in self._global_keywords) or
                any(kw in lower for kw in self._domain_keywords)
            )
            if already_known:
                continue

            # FIX-B3: RPG story content guard — tuyệt đối không approve
            if _is_rpg_story_content(line):
                logger.debug(
                    "[AdsFilter] RPG content blocked from candidates: %r", line[:60]
                )
                continue

            # FIX-B1: Auto-approve CHỈ khi là JS injection (100% ads)
            if _JS_INJECTION_RE.search(line):
                if len(auto_add) < max_results:
                    auto_add.append(line)
            else:
                # Tất cả còn lại → AI verify, bất kể count cao đến mấy
                if len(ai_verify) < max_results:
                    ai_verify.append(line)

        return auto_add, ai_verify

    def clear_session_log(self) -> None:
        self._session_log.clear()

    # ── Inject từ profile ─────────────────────────────────────────────────

    def inject_from_profile(self, profile: "SiteProfile") -> int:
        added = 0
        for kw in profile.get("ads_keywords_learned") or []:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            # FIX-B1: Không inject RPG story content keywords từ profile
            # (guard against bad data that may have been learned in previous sessions)
            if _is_rpg_story_content(kw_lower):
                logger.warning(
                    "[AdsFilter] Skipping RPG content keyword from profile: %r", kw_lower
                )
                continue
            if (kw_lower and
                    kw_lower not in self._global_keywords and
                    kw_lower not in self._domain_keywords):
                self._domain_keywords.add(kw_lower)
                added += 1
        return added

    def add_keywords(self, keywords: list[str], to_domain: bool = True) -> int:
        added      = 0
        use_domain = to_domain and bool(self._domain)

        for kw in keywords:
            if not isinstance(kw, str):
                continue
            kw_lower = kw.lower().strip()
            if not kw_lower:
                continue
            if kw_lower in _GENERIC_KEYWORD_BLACKLIST:
                continue
            if len(kw_lower) < 8:
                continue
            if kw_lower in self._global_keywords or kw_lower in self._domain_keywords:
                continue
            # FIX-B1: Guard RPG content trước khi add
            if _is_rpg_story_content(kw_lower):
                logger.warning(
                    "[AdsFilter] Blocked RPG content from being added as keyword: %r",
                    kw_lower,
                )
                continue
            if use_domain:
                self._domain_keywords.add(kw_lower)
            else:
                self._global_keywords.add(kw_lower)
            added += 1

        return added

    def apply_verified(self, confirmed_lines: list[str]) -> int:
        return self.add_keywords(confirmed_lines, to_domain=True)

    # ── Core detection ────────────────────────────────────────────────────

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

    # ── Save/Load DB ──────────────────────────────────────────────────────

    def save(self) -> None:
        """Lưu keywords/patterns xuống file. Thread-safe."""
        with _ADS_DB_WRITE_LOCK:
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

            existing.setdefault("global",  {"keywords": [], "patterns": []})
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

    # ── Persistent review file ────────────────────────────────────────────

    def save_pending_review(
        self,
        domain_slug     : str,
        verified_results: dict[str, bool] | None = None,
    ) -> str | None:
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
            verified = (verified_results or {}).get(line)
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
                    "line"      : line,
                    "count"     : info["count"],
                    "story_urls": info["urls"],
                    "ai_verified": verified,
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
        freq_cnt  = len(self._freq_counter)
        notified  = len(self._notified_ads)
        if self._domain:
            return (
                f"{total_kw}kw "
                f"({len(self._global_keywords)}g+{len(self._domain_keywords)}local)"
                f"/{total_pat}pat | {freq_cnt} tracked | {notified} notified"
            )
        return f"{total_kw}kw/{total_pat}pat | {freq_cnt} tracked | {notified} notified"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_bucket(
    bucket  : dict,
    kw_set  : set[str],
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