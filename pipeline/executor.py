"""
pipeline/executor.py

Fix P0-1: _make_block() flatten params trước khi truyền vào factory.
Fix P1-7: PipelineRunner.from_profile() log warning rõ ràng thay vì silent None.
Fix P2-15: xóa dead import context_summary (không có call site nào trong codebase).
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

_DASH_NORM = re.compile(r"[–—‐]")
_WS_NORM   = re.compile(r"\s+")


def _make_vote_key(title: str) -> str:
    """Normalize title để làm dict key trong vote — dash variants + whitespace."""
    key = title.lower()
    key = _DASH_NORM.sub("-", key)
    key = _WS_NORM.sub(" ", key).strip()
    return key


# ── Block factories ────────────────────────────────────────────────────────────

def _make_block(chain_type: str, step: StepConfig):
    """
    Factory: tạo block instance từ chain_type + StepConfig.

    Fix P0-1: StepConfig.to_dict() → {"type": "selector", "params": {"selector": "..."}}
    Tất cả from_config() đọc flat format. Unpack ở đây — single choke-point,
    fix toàn bộ 5 chain types mà không cần đụng 15+ from_config() methods.
    """
    _d  = step.to_dict()
    cfg = {"type": _d["type"], **_d.get("params", {})}

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
    """Parse HTML → BeautifulSoup và apply html_filter."""
    if not ctx.html:
        return

    profile          = ctx.profile
    remove_selectors = profile.get("remove_selectors") or []
    content_selector = profile.get("content_selector")
    title_selector   = profile.get("title_selector")

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
    Mặc định: first-wins. Chế độ title_vote: chạy hết, chọn bằng weighted vote.
    """

    def __init__(self, chain: ChainConfig, special_mode: str = "") -> None:
        self.chain        = chain
        self.special_mode = special_mode

    async def run(self, ctx: PipelineContext) -> BlockResult:
        if self.special_mode == "title_vote":
            return await self._run_title_vote(ctx)
        return await self._run_first_wins(ctx)

    async def _run_first_wins(self, ctx: PipelineContext) -> BlockResult:
        last_result = BlockResult.failed("chain is empty")

        for step in self.chain.steps:
            try:
                block = _make_block(self.chain.chain_type, step)
            except ValueError as e:
                logger.warning("[Chain:%s] unknown block %r: %s", self.chain.chain_type, step.type, e)
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
                logger.debug("[Chain:%s] ✓ %s (conf=%.2f dur=%.0fms)",
                             self.chain.chain_type, step.type,
                             result.confidence, result.duration_ms)
                return result
            logger.debug("[Chain:%s] ✗ %s — %s",
                         self.chain.chain_type, step.type, result.error or "failed")

        logger.debug("[Chain:%s] all %d steps failed", self.chain.chain_type, len(self.chain.steps))
        return last_result

    async def _run_title_vote(self, ctx: PipelineContext) -> BlockResult:
        """Confidence-weighted vote với dash-normalized keys (Fix M2)."""
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

        vote_weights  : dict[str, float] = {}
        original_case : dict[str, str]   = {}

        for title, conf in zip(candidates, confidences):
            key = _make_vote_key(title)
            vote_weights[key] = vote_weights.get(key, 0.0) + conf
            if key not in original_case or len(title) > len(original_case[key]):
                original_case[key] = title

        top_weight_val = max(vote_weights.values())
        tied_keys      = [k for k, w in vote_weights.items() if w == top_weight_val]
        winner_key     = max(tied_keys, key=len) if len(tied_keys) > 1 else tied_keys[0]
        winner         = original_case[winner_key]
        total_weight   = sum(vote_weights.values())
        confidence     = vote_weights[winner_key] / total_weight if total_weight > 0 else 0.0

        return BlockResult.success(
            data         = winner,
            method_used  = "title_vote",
            confidence   = round(confidence, 3),
            vote_weights = {original_case[k]: round(w, 3) for k, w in vote_weights.items()},
            candidates   = candidates,
        )


# ── PipelineRunner ─────────────────────────────────────────────────────────────

class PipelineRunner:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    async def run(
        self,
        url            : str,
        profile        : dict,
        progress       : dict,
        pool           : Any = None,
        pw_pool        : Any = None,
        ai_limiter     : Any = None,
        prefetched_html: str | None = None,
    ) -> PipelineContext:
        from pipeline.context import make_context

        ctx         = make_context(url=url, profile=dict(profile), progress=progress)
        ctx.runtime = RuntimeContext.create(pool=pool, pw_pool=pw_pool, ai_limiter=ai_limiter)

        # 1. Fetch
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

        # 2. Parse + filter
        await build_soup(ctx)
        if ctx.soup is None:
            logger.warning("[Runner] soup is None after parse for %s", url)
            return ctx

        # 3. Extract content
        extract_result = await ChainExecutor(self.config.extract_chain).run(ctx)
        if extract_result.ok:
            ctx.content       = extract_result.data
            ctx.selector_used = extract_result.metadata.get("selector")

        # 4. Extract title
        title_result = await ChainExecutor(
            self.config.title_chain, special_mode="title_vote"
        ).run(ctx)
        if title_result.ok:
            ctx.title_clean = title_result.data
            ctx.title_raw   = title_result.data

        # 5. Navigate
        nav_result = await ChainExecutor(self.config.nav_chain).run(ctx)
        if nav_result.ok:
            ctx.next_url   = nav_result.data
            ctx.nav_method = nav_result.method_used

        # 6. Validate
        await ChainExecutor(self.config.validate_chain).run(ctx)

        return ctx

    @classmethod
    def from_profile(cls, profile: dict) -> "PipelineRunner | None":
        """
        Tạo PipelineRunner từ profile dict.

        Fix P1-7: log warning rõ ràng cho mọi None path.
        Trước: silent return None → scraping dùng default pipeline không warning
        → toàn bộ AI-learned selectors bị ignore, impossible to debug.
        """
        domain        = profile.get("domain", "unknown")
        pipeline_data = profile.get("pipeline")

        if not pipeline_data:
            logger.warning(
                "[Runner] %s: không có 'pipeline' key trong profile — "
                "fallback default. Chạy lại learning phase hoặc migration.",
                domain,
            )
            return None

        if not isinstance(pipeline_data, dict):
            logger.warning(
                "[Runner] %s: pipeline config sai kiểu (%s) — fallback default.",
                domain, type(pipeline_data).__name__,
            )
            return None

        try:
            config = PipelineConfig.from_dict(pipeline_data)
            return cls(config)
        except Exception as e:
            logger.warning(
                "[Runner] %s: load pipeline config thất bại (%s) — fallback default.",
                domain, e,
            )
            return None

    @classmethod
    def default(cls, domain: str) -> "PipelineRunner":
        return cls(PipelineConfig.default_for_domain(domain))


# ── Convenience shortcut ───────────────────────────────────────────────────────

async def run_chapter(
    url            : str,
    profile        : dict,
    progress       : dict,
    pool           : Any = None,
    pw_pool        : Any = None,
    ai_limiter     : Any = None,
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