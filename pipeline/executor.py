"""
pipeline/executor.py — ChainExecutor và PipelineRunner.

v2 changes:
  EXEC-1: PipelineRunner inject RuntimeContext vào ctx.runtime thay vì
          nhét live objects vào ctx.profile dict (anti-pattern cũ).

  EXEC-2: Title vote dùng confidence-weighted voting thay vì unweighted.
          Trước: "Chapter 5" (slug, conf=0.40) có thể đánh bại
                 "Chapter 5 – The Beginning" (selector, conf=0.95).
          Bây giờ: confidence của mỗi block là trọng số vote của nó.

  EXEC-3: _build_soup đổi thành build_soup (public).

  EXEC-4: Sau fetch chain, executor đọc BlockResult.metadata["js_heavy"]
          và set ctx.detected_js_heavy — KHÔNG để block tự mutate profile.

Fix M2: _run_title_vote() dùng _make_vote_key() để normalize dash variants
  trước khi so sánh. "Chapter 5 – The Rise" và "Chapter 5 - The Rise"
  (em-dash vs hyphen) giờ được coi là cùng một title, votes không bị split.
  original_case dict vẫn giữ display version đầy đủ nhất (dài nhất).
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from pipeline.base import (
    BlockResult, BlockStatus, BlockType,
    ChainConfig, PipelineConfig, PipelineContext,
    RuntimeContext, StepConfig,
)

logger = logging.getLogger(__name__)

# ── Dash normalization cho title vote key ─────────────────────────────────────
# Các ký tự này semantically giống nhau khi so sánh titles:
#   – (en-dash U+2013), — (em-dash U+2014), ‐ (hyphen U+2010) → - (ASCII hyphen)
# Whitespace collapse để tránh "Chapter  5" vs "Chapter 5"
_DASH_NORM = re.compile(r"[–—‐]")
_WS_NORM   = re.compile(r"\s+")


def _make_vote_key(title: str) -> str:
    """
    Tạo normalized key để so sánh titles trong vote.

    Chỉ dùng cho DICT KEY — không thay đổi display string.
    Normalize:
        - Lowercase
        - Em-dash / en-dash → ASCII hyphen
        - Whitespace collapse

    Examples:
        _make_vote_key("Chapter 5 – The Rise") → "chapter 5 - the rise"
        _make_vote_key("Chapter 5 - The Rise") → "chapter 5 - the rise"  ← same key
        _make_vote_key("Chapter  5  —  Rise")  → "chapter 5 - rise"
    """
    key = title.lower()
    key = _DASH_NORM.sub("-", key)
    key = _WS_NORM.sub(" ", key).strip()
    return key


# ── Block factories ────────────────────────────────────────────────────────────

def _make_block(chain_type: str, step: StepConfig):
    """Factory: tạo block instance từ chain_type + StepConfig (lazy import)."""
    cfg = step.to_dict()

    if chain_type == "fetch":
        from pipeline.fetcher import make_fetch_block
        return make_fetch_block(cfg)
    if chain_type == "extract":
        from pipeline.extractor import make_extract_block
        return make_extract_block(cfg)
    if chain_type == "navigate":
        from pipeline.navigator import make_nav_block
        return make_nav_block(cfg)
    if chain_type == "title":
        from pipeline.title_extractor import make_title_block
        return make_title_block(cfg)
    if chain_type == "validate":
        from pipeline.validator import make_validate_block
        return make_validate_block(cfg)

    raise ValueError(f"Unknown chain_type: {chain_type!r}")


# ── HTML filter + soup builder ────────────────────────────────────────────────

async def build_soup(ctx: PipelineContext) -> None:
    """
    Parse HTML → BeautifulSoup và apply html_filter.
    Kết quả ghi vào ctx.soup.
    """
    if not ctx.html:
        return

    profile           = ctx.profile
    remove_selectors  = profile.get("remove_selectors") or []
    content_selector  = profile.get("content_selector")
    title_selector    = profile.get("title_selector")

    try:
        from core.html_filter import prepare_soup
        ctx.soup = await asyncio.to_thread(
            prepare_soup,
            ctx.html,
            remove_selectors,
            content_selector,
            title_selector,
        )
    except Exception as e:
        logger.warning("[Executor] html_filter thất bại, dùng raw parse: %s", e)
        ctx.soup = BeautifulSoup(ctx.html, "html.parser")


# ── ChainExecutor ──────────────────────────────────────────────────────────────

class ChainExecutor:
    """
    Thực thi một chain (ordered list of strategies).

    Chế độ mặc định: first-wins — dừng tại step đầu tiên có ok result.
    Chế độ title_vote: chạy hết tất cả blocks, chọn bằng weighted vote.
    """

    def __init__(self, chain: ChainConfig, special_mode: str = "") -> None:
        self.chain        = chain
        self.special_mode = special_mode

    async def run(self, ctx: PipelineContext) -> BlockResult:
        if self.special_mode == "title_vote":
            return await self._run_title_vote(ctx)
        return await self._run_first_wins(ctx)

    async def _run_first_wins(self, ctx: PipelineContext) -> BlockResult:
        """Standard: dừng tại step đầu tiên thành công."""
        last_result = BlockResult.failed("chain is empty")

        for step in self.chain.steps:
            try:
                block = _make_block(self.chain.chain_type, step)
            except ValueError as e:
                logger.warning(
                    "[Chain:%s] unknown block %r: %s",
                    self.chain.chain_type, step.type, e,
                )
                continue

            block_key = f"{self.chain.chain_type}:{step.type}"

            try:
                result = await block.execute(ctx)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                result = BlockResult.failed(str(e) or repr(e), method_used=step.type)

            result.method_used = result.method_used or step.type
            ctx.record(block_key, result)
            last_result = result

            if result.status == BlockStatus.SKIPPED:
                continue

            if result.ok:
                logger.debug(
                    "[Chain:%s] ✓ %s (conf=%.2f dur=%.0fms)",
                    self.chain.chain_type, step.type,
                    result.confidence, result.duration_ms,
                )
                return result

            logger.debug(
                "[Chain:%s] ✗ %s — %s",
                self.chain.chain_type, step.type, result.error or "failed",
            )

        logger.debug(
            "[Chain:%s] all %d steps failed",
            self.chain.chain_type, len(self.chain.steps),
        )
        return last_result

    async def _run_title_vote(self, ctx: PipelineContext) -> BlockResult:
        """
        Title vote: chạy hết tất cả title blocks, chọn bằng confidence-weighted vote.

        Fix M2: dùng _make_vote_key() thay vì title.lower() làm dict key.
        Lý do: "Chapter 5 – The Rise" (em-dash) và "Chapter 5 - The Rise"
        (hyphen) là cùng một title nhưng tạo ra 2 key khác nhau với lower().
        Votes bị split → winner sai (block confidence thấp hơn có thể thắng).

        _make_vote_key() normalize dash variants + whitespace trước khi so sánh.
        original_case dict vẫn giữ display version tốt nhất (ưu tiên dài hơn).

        Cơ chế vote:
        - Mỗi block vote cho title của mình với trọng số = confidence của nó.
        - Winner = title có tổng trọng số lớn nhất.
        - Tie-break: key dài nhất (thường đầy đủ hơn).
        - Final confidence = tổng trọng số của winner / tổng trọng số tất cả.

        Ví dụ (trước fix — bị split):
            url_slug   "Chapter 5 - The Rise"   conf=0.40  key="chapter 5 - the rise"
            title_tag  "Chapter 5 - The Rise"   conf=0.65  key="chapter 5 - the rise"
            selector   "Chapter 5 – The Rise"   conf=0.95  key="chapter 5 – the rise"  ← KHÁC!
            → "chapter 5 - the rise" weight=1.05 vs "chapter 5 – the rise" weight=0.95
            → Winner sai: title_tag/url_slug thắng dù selector có conf cao hơn

        Ví dụ (sau fix — merged):
            url_slug   "Chapter 5 - The Rise"   conf=0.40  key="chapter 5 - the rise"
            title_tag  "Chapter 5 - The Rise"   conf=0.65  key="chapter 5 - the rise"
            selector   "Chapter 5 – The Rise"   conf=0.95  key="chapter 5 - the rise"  ← SAME
            → "chapter 5 - the rise" total_weight=2.00
            → Winner đúng, display version = "Chapter 5 – The Rise" (dài nhất)
        """
        candidates  : list[str]   = []
        confidences : list[float] = []

        for step in self.chain.steps:
            try:
                block = _make_block(self.chain.chain_type, step)
            except ValueError:
                continue

            block_key = f"title:{step.type}"
            try:
                result = await block.execute(ctx)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                result = BlockResult.failed(str(e) or repr(e))

            ctx.record(block_key, result)

            if result.ok and isinstance(result.data, str):
                title = result.data.strip()
                if len(title) >= 3:
                    candidates.append(title)
                    confidences.append(result.confidence)

        if not candidates:
            return BlockResult.failed("all title blocks failed")

        # Confidence-weighted voting với normalized key (Fix M2)
        vote_weights  : dict[str, float] = {}   # norm_key → total weight
        original_case : dict[str, str]   = {}   # norm_key → best display version

        for title, conf in zip(candidates, confidences):
            key = _make_vote_key(title)           # ← normalize: dash + whitespace
            vote_weights[key]  = vote_weights.get(key, 0.0) + conf

            # Giữ display version dài hơn (thường đầy đủ hơn)
            if key not in original_case or len(title) > len(original_case[key]):
                original_case[key] = title

        # Chọn winner: tổng weight cao nhất
        top_weight_val = max(vote_weights.values())
        tied_keys = [k for k, w in vote_weights.items() if w == top_weight_val]

        # Tie-break: key dài nhất (normalize key dài hơn = title phong phú hơn)
        winner_key = max(tied_keys, key=len) if len(tied_keys) > 1 else tied_keys[0]

        winner       = original_case[winner_key]
        total_weight = sum(vote_weights.values())
        confidence   = vote_weights[winner_key] / total_weight if total_weight > 0 else 0.0

        return BlockResult.success(
            data         = winner,
            method_used  = "title_vote",
            confidence   = round(confidence, 3),
            vote_weights = {original_case[k]: round(w, 3) for k, w in vote_weights.items()},
            candidates   = candidates,
        )


# ── PipelineRunner ─────────────────────────────────────────────────────────────

class PipelineRunner:
    """
    Orchestrate toàn bộ pipeline cho một chapter.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    async def run(
        self,
        url            : str,
        profile        : dict,
        progress       : dict,
        pool           : Any   = None,
        pw_pool        : Any   = None,
        ai_limiter     : Any   = None,
        prefetched_html: str | None = None,
    ) -> PipelineContext:
        from pipeline.context import make_context

        ctx = make_context(url=url, profile=dict(profile), progress=progress)

        ctx.runtime = RuntimeContext.create(
            pool       = pool,
            pw_pool    = pw_pool,
            ai_limiter = ai_limiter,
        )

        # ── 1. Fetch ──────────────────────────────────────────────────────────
        if prefetched_html is not None:
            ctx.html         = prefetched_html
            ctx.status_code  = 200
            ctx.fetch_method = "prefetched"
        else:
            fetch_result = await ChainExecutor(self.config.fetch_chain).run(ctx)
            if not fetch_result.ok:
                logger.warning("[Runner] fetch failed for %s: %s", url, fetch_result.error)
                return ctx
            ctx.html         = fetch_result.data
            ctx.fetch_method = fetch_result.method_used
            ctx.status_code  = fetch_result.metadata.get("status_code", 200)

            if fetch_result.metadata.get("js_heavy"):
                ctx.detected_js_heavy = True
                logger.info("[Runner] js_heavy detected for %s", url)

        # ── 2. Parse + filter HTML ────────────────────────────────────────────
        await build_soup(ctx)

        if ctx.soup is None:
            logger.warning("[Runner] soup is None after parse for %s", url)
            return ctx

        # ── 3. Extract content ────────────────────────────────────────────────
        extract_result = await ChainExecutor(self.config.extract_chain).run(ctx)
        if extract_result.ok:
            ctx.content       = extract_result.data
            ctx.selector_used = extract_result.metadata.get("selector")

        # ── 4. Extract title (confidence-weighted vote với normalized keys) ───
        title_result = await ChainExecutor(
            self.config.title_chain, special_mode="title_vote"
        ).run(ctx)
        if title_result.ok:
            ctx.title_clean = title_result.data
            ctx.title_raw   = title_result.data

        # ── 5. Navigate ───────────────────────────────────────────────────────
        nav_result = await ChainExecutor(self.config.nav_chain).run(ctx)
        if nav_result.ok:
            ctx.next_url   = nav_result.data
            ctx.nav_method = nav_result.method_used

        # ── 6. Validate ───────────────────────────────────────────────────────
        await ChainExecutor(self.config.validate_chain).run(ctx)

        return ctx

    @classmethod
    def from_profile(cls, profile: dict) -> "PipelineRunner | None":
        pipeline_data = profile.get("pipeline")
        if not pipeline_data or not isinstance(pipeline_data, dict):
            return None
        try:
            config = PipelineConfig.from_dict(pipeline_data)
            return cls(config)
        except Exception as e:
            logger.warning("[Runner] cannot load pipeline config: %s", e)
            return None

    @classmethod
    def default(cls, domain: str) -> "PipelineRunner":
        return cls(PipelineConfig.default_for_domain(domain))


# ── Convenience shortcut ───────────────────────────────────────────────────────

async def run_chapter(
    url            : str,
    profile        : dict,
    progress       : dict,
    pool           : Any   = None,
    pw_pool        : Any   = None,
    ai_limiter     : Any   = None,
    prefetched_html: str | None = None,
) -> PipelineContext:
    """Shortcut: tạo PipelineRunner từ profile và chạy một chapter."""
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lower()

    runner = PipelineRunner.from_profile(profile) or PipelineRunner.default(domain)
    return await runner.run(
        url             = url,
        profile         = profile,
        progress        = progress,
        pool            = pool,
        pw_pool         = pw_pool,
        ai_limiter      = ai_limiter,
        prefetched_html = prefetched_html,
    )