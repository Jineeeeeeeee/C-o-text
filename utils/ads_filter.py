
from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter
from pathlib import Path

from config import ADS_DB_FILE

logger = logging.getLogger(__name__)

_MIN_LINE_LEN = 10
_MAX_LINE_LEN = 300

# Inline suspect thresholds (Q2 confirmed)
_INLINE_AI_THRESHOLD   = 3   # >= 3 files → AI verify
_INLINE_AUTO_THRESHOLD = 8   # >= 8 files → auto-add


class AdsFilter:

    def __init__(self, domain: str, known_keywords: set[str]) -> None:
        self._domain   = domain
        self._keywords : set[str] = known_keywords
        self._suspects : Counter  = Counter()
        self._file_counter: Counter = Counter()
        # Fix ADS-A: separate counter for inline (middle-of-content) occurrences
        self._inline_file_counter: Counter = Counter()
        self._pending_review: dict = {}
        self._new_suspects: set[str] = set()

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def load(cls, domain: str) -> "AdsFilter":
        global_kws: set[str] = set()
        domain_kws: set[str] = set()

        if os.path.exists(ADS_DB_FILE):
            try:
                with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    global_kws = set(data.get("global", []))
                    domain_kws = set(data.get(domain, []))
            except Exception as e:
                logger.warning("[Ads] load failed: %s", e)

        return cls(domain=domain, known_keywords=global_kws | domain_kws)

    def inject_from_profile(self, profile: dict) -> int:
        kws = profile.get("ads_keywords_learned") or []
        before = len(self._keywords)
        for kw in kws:
            if isinstance(kw, str) and kw.strip():
                self._keywords.add(kw.lower().strip())
        return len(self._keywords) - before

    # ── Filtering ─────────────────────────────────────────────────────────────

    def filter(self, content: str, chapter_url: str = "") -> str:
        if not self._keywords:
            return content

        lines   = content.splitlines()
        cleaned = []
        for line in lines:
            lo = line.lower().strip()
            if lo and any(kw in lo for kw in self._keywords):
                logger.debug("[Ads] Filtered: %r", line[:80])
                continue
            cleaned.append(line)

        return "\n".join(cleaned)

    def scan_edges_for_suspects(
        self,
        content     : str,
        chapter_url : str = "",
        chapter_file: str = "",
    ) -> None:
        """Quét đầu/cuối chapter để tìm suspect lines."""
        lines = [l.strip() for l in content.splitlines() if l.strip()]
        if not lines:
            return

        edge = min(5, len(lines))
        candidates = lines[:edge] + lines[-edge:]

        for line in candidates:
            lo = line.lower()
            if _MIN_LINE_LEN <= len(lo) <= _MAX_LINE_LEN:
                if lo not in self._keywords:
                    self._suspects[lo] += 1
                    self._file_counter[lo] += 1

    def scan_inline_for_watermarks(
        self,
        content    : str,
        chapter_file: str = "",
        edge_skip  : int = 5,
    ) -> None:
        """
        [NEW — Fix ADS-A] Quét phần GIỮA content để detect inline watermarks.

        Logic: A line appearing in the MIDDLE of content across multiple chapters
        cannot be story content → it's a watermark injected by the site.

        Cross-chapter comparison (user Q2):
          - >= _INLINE_AI_THRESHOLD (3) files → AI verify candidate
          - >= _INLINE_AUTO_THRESHOLD (8) files → auto-add without AI

        Tracking: _inline_file_counter counts each unique line ONCE per file,
        regardless of how many times it appears in that file. This gives a
        true "appears in N chapters" count for threshold comparison.

        Args:
            content:      Extracted chapter content (post-ads-filter)
            chapter_file: Path of chapter file (for logging, not used in counting)
            edge_skip:    Lines to skip at start/end (these are covered by
                          scan_edges_for_suspects already)
        """
        lines = [l.strip() for l in content.splitlines() if l.strip()]
        n     = len(lines)

        # Need enough lines to have a meaningful "middle"
        if n < edge_skip * 2 + 2:
            return

        # Middle = skip first and last edge_skip lines
        middle_lines = lines[edge_skip: n - edge_skip]

        # Track which lines we've already counted for THIS chapter
        # (count each line once per chapter, not once per occurrence)
        seen_in_chapter: set[str] = set()

        for line in middle_lines:
            lo = line.lower()

            # Basic length filter
            if not (_MIN_LINE_LEN <= len(lo) <= _MAX_LINE_LEN):
                continue

            # Skip already-confirmed ads
            if lo in self._keywords:
                continue

            # Skip lines we've already counted for this chapter
            if lo in seen_in_chapter:
                continue

            seen_in_chapter.add(lo)
            self._inline_file_counter[lo] += 1

    # ── Candidate retrieval ───────────────────────────────────────────────────

    def get_candidates_by_frequency(
        self,
        auto_threshold: int = 10,
        min_count     : int = 3,
        max_results   : int = 20,
    ) -> tuple[list[str], list[str]]:
        """
        Returns (auto_candidates, ai_candidates).
        auto: >= auto_threshold occurrences (edge OR inline)
        ai:   >= min_count but < auto_threshold (edge OR inline)

        Fix ADS-A: inline suspects contribute with 1.5× weight since
        inline is a stronger signal than edge occurrence (inline lines
        in multiple chapters are almost certainly watermarks, not nav).
        """
        auto: list[str] = []
        ai  : list[str] = []
        seen: set[str]  = set()

        # Combine edge + inline suspects with inline boost
        combined: Counter = Counter()
        combined.update(self._suspects)
        for line, count in self._inline_file_counter.items():
            # 1.5× weight for inline (stronger watermark signal)
            combined[line] = combined.get(line, 0) + int(count * 1.5)

        for line, count in combined.most_common(max_results * 2):
            if line in self._keywords or line in seen:
                continue
            seen.add(line)

            if count >= auto_threshold:
                auto.append(line)
            elif count >= min_count:
                ai.append(line)

            if len(auto) + len(ai) >= max_results:
                break

        return auto[:max_results], ai[:max_results]

    def get_new_frequency_suspects(
        self,
        min_files  : int = 5,
        max_results: int = 20,
    ) -> list[str]:
        """
        Lines xuất hiện trong >= min_files chapters, chưa confirmed.

        Fix ADS-A: Also surface inline suspects with lower threshold
        (_INLINE_AI_THRESHOLD = 3) since inline = stronger signal.
        Inline threshold override: max(3, min_files // 2).
        """
        result: list[str] = []
        seen  : set[str]  = set()

        # Edge suspects (original behavior)
        for line, count in self._file_counter.most_common():
            if line in self._keywords or line in seen:
                continue
            if count >= min_files:
                seen.add(line)
                result.append(line)
                self._new_suspects.add(line)
            if len(result) >= max_results:
                break

        # Fix ADS-A: Inline suspects with lower threshold
        inline_threshold = max(_INLINE_AI_THRESHOLD, min_files // 2)
        for line, count in self._inline_file_counter.most_common():
            if line in self._keywords or line in seen:
                continue
            if count >= inline_threshold:
                seen.add(line)
                result.append(line)
                self._new_suspects.add(line)
            if len(result) >= max_results:
                break

        return result[:max_results]

    # ── Applying verified results ─────────────────────────────────────────────

    def apply_verified(self, lines: list[str]) -> int:
        added = 0
        for line in lines:
            lo = line.lower().strip()
            if lo and lo not in self._keywords:
                self._keywords.add(lo)
                added += 1
        return added

    def save_pending_review(
        self,
        domain_slug     : str,
        verified_results: dict | None = None,
    ) -> None:
        if verified_results:
            self._pending_review.update(verified_results)

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self) -> None:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(ADS_DB_FILE)), exist_ok=True)
            data: dict = {}
            if os.path.exists(ADS_DB_FILE):
                with open(ADS_DB_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)

            existing = set(data.get(self._domain, []))
            merged   = sorted(existing | self._keywords)
            data[self._domain] = merged

            with open(ADS_DB_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.warning("[Ads] save failed: %s", e)

    @property
    def stats(self) -> str:
        return (
            f"known={len(self._keywords)} "
            f"edge_suspects={len(self._suspects)} "
            f"inline_suspects={len(self._inline_file_counter)}"
        )

    # ── Post-processing ───────────────────────────────────────────────────────

    @staticmethod
    def post_process_directory(confirmed_lines: list[str], output_dir: str) -> int:
        if not confirmed_lines or not os.path.isdir(output_dir):
            return 0

        patterns = [line.lower().strip() for line in confirmed_lines if line.strip()]
        total_removed = 0

        for fname in os.listdir(output_dir):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(output_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                cleaned  = [l for l in lines if not any(p in l.lower() for p in patterns)]
                removed  = len(lines) - len(cleaned)

                if removed > 0:
                    with open(fpath, "w", encoding="utf-8", newline="\n") as f:
                        f.writelines(cleaned)
                    total_removed += removed

            except Exception as e:
                logger.debug("[Ads] post_process error on %s: %s", fname, e)

        return total_removed