"""
pipeline/executor.py

Batch B: PipelineRunner đọc trực tiếp từ SiteProfile flat fields.
  Trước: from_profile() deserialize profile["pipeline"] → PipelineConfig →
         StepConfig → _make_block(). Roundtrip qua JSON là root cause bug M4.
  Sau:   from_profile() nhận profile dict, _*_blocks() build danh sách block
         trực tiếp từ content_selector, next_selector, nav_type, v.v.
         Không còn _make_block(), không còn StepConfig/ChainConfig import.

Batch B: ChainExecutor nhận list[ScraperBlock] thay vì ChainConfig.
  Chains được build bởi PipelineRunner._*_blocks() methods.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from pipeline.base import (
    BlockResult, BlockStatus,
    PipelineContext, RuntimeContext, ScraperBlock,
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
    Thực thi một chain (ordered list of blocks).
    Mặc định: first-wins. Chế độ title_vote: chạy hết, chọn bằng weighted vote.

    Batch B: nhận list[ScraperBlock] trực tiếp thay vì ChainConfig.
    """

    def __init__(
        self,
        blocks      : list[ScraperBlock],
        chain_type  : str = "",
        special_mode: str = "",
    ) -> None:
        self.blocks       = blocks
        self.chain_type   = chain_type
        self.special_mode = special_mode

    async def run(self, ctx: PipelineContext) -> BlockResult:
        if self.special_mode == "title_vote":
            return await self._run_title_vote(ctx)
        return await self._run_first_wins(ctx)

    async def _run_first_wins(self, ctx: PipelineContext) -> BlockResult:
        last_result = BlockResult.failed("chain is empty")

        for block in self.blocks:
            block_key = f"{self.chain_type}:{block.name}"
            try:
                result = await block.execute(ctx)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                result = BlockResult.failed(str(e) or repr(e), method_used=block.name)

            result.method_used = result.method_used or block.name
            ctx.record(block_key, result)
            last_result = result

            if result.status == BlockStatus.SKIPPED:
                continue
            if result.ok:
                logger.debug("[Chain:%s] ✓ %s (conf=%.2f dur=%.0fms)",
                             self.chain_type, block.name,
                             result.confidence, result.duration_ms)
                return result
            logger.debug("[Chain:%s] ✗ %s — %s",
                         self.chain_type, block.name, result.error or "failed")

        logger.debug("[Chain:%s] all %d blocks failed", self.chain_type, len(self.blocks))
        return last_result

    async def _run_title_vote(self, ctx: PipelineContext) -> BlockResult:
        """
        Confidence-weighted vote với dash-normalized keys.
        Đếm riêng skipped_count và failed_count để error message rõ ràng.
        """
        candidates  : list[str]   = []
        confidences : list[float] = []
        skipped_count: int = 0
        failed_count : int = 0
        total_blocks : int = len(self.blocks)

        for block in self.blocks:
            block_key = f"title:{block.name}"
            try:
                result = await block.execute(ctx)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                result = BlockResult.failed(str(e) or repr(e))

            ctx.record(block_key, result)

            if result.status == BlockStatus.SKIPPED:
                skipped_count += 1
            elif result.status == BlockStatus.FAILED:
                failed_count += 1
            elif result.ok and isinstance(result.data, str):
                title = result.data.strip()
                if len(title) >= 3:
                    candidates.append(title)
                    confidences.append(result.confidence)

        if not candidates:
            if skipped_count == total_blocks:
                msg = f"all {total_blocks} title blocks skipped (no soup or no html?)"
            elif failed_count == total_blocks:
                msg = f"all {total_blocks} title blocks failed"
            else:
                msg = f"{skipped_count} skipped, {failed_count} failed — no title found"
            return BlockResult.failed(msg)

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
    """
    Batch B: đọc trực tiếp từ SiteProfile flat fields.

    Không còn deserialization qua PipelineConfig/StepConfig/ChainConfig.
    Mỗi _*_blocks() method build danh sách block từ profile fields:
      - content_selector   → SelectorExtractBlock
      - next_selector      → SelectorNavBlock
      - title_selector     → SelectorTitleBlock
      - requires_playwright → PlaywrightFetchBlock first vs HybridFetchBlock first
      - nav_type           → fallback nav block selection

    Profile có thể thiếu bất kỳ field nào (empty profile → chỉ dùng heuristics).
    """

    def __init__(self, profile: dict) -> None:
        self._profile = profile

    # ── Chain builders ─────────────────────────────────────────────────────────

    def _fetch_blocks(self) -> list[ScraperBlock]:
        from pipeline.fetcher import HybridFetchBlock, PlaywrightFetchBlock
        if self._profile.get("requires_playwright", False):
            # JS-heavy site: Playwright first, Hybrid as fallback
            return [PlaywrightFetchBlock(), HybridFetchBlock()]
        return [HybridFetchBlock(), PlaywrightFetchBlock()]

    def _extract_blocks(self) -> list[ScraperBlock]:
        from pipeline.extractor import (
            SelectorExtractBlock, JsonLdExtractBlock, DensityHeuristicBlock,
            FallbackListExtractBlock, AIExtractBlock,
        )
        blocks: list[ScraperBlock] = []
        sel = self._profile.get("content_selector")
        if sel:
            blocks.append(SelectorExtractBlock(selector=sel))
        blocks += [
            JsonLdExtractBlock(),
            DensityHeuristicBlock(),
            FallbackListExtractBlock(),
            AIExtractBlock(),
        ]
        return blocks

    def _title_blocks(self) -> list[ScraperBlock]:
        from pipeline.title_extractor import (
            SelectorTitleBlock, H1TitleBlock, TitleTagBlock,
            OgTitleBlock, UrlSlugTitleBlock,
        )
        blocks: list[ScraperBlock] = []
        sel = self._profile.get("title_selector")
        if sel:
            blocks.append(SelectorTitleBlock(selector=sel))
        blocks += [H1TitleBlock(), TitleTagBlock(), OgTitleBlock(), UrlSlugTitleBlock()]
        return blocks

    def _nav_blocks(self) -> list[ScraperBlock]:
        from pipeline.navigator import (
            RelNextNavBlock, SelectorNavBlock, AnchorTextNavBlock,
            SlugIncrementNavBlock, FanficNavBlock, SelectDropdownNavBlock, AINavBlock,
        )
        blocks: list[ScraperBlock] = [RelNextNavBlock()]
        next_sel = self._profile.get("next_selector")
        nav_type = (self._profile.get("nav_type") or "").lower()

        if next_sel:
            blocks.append(SelectorNavBlock(selector=next_sel))
        elif nav_type == "slug_increment":
            blocks.append(SlugIncrementNavBlock())
        elif nav_type == "fanfic":
            blocks.append(FanficNavBlock())
        elif nav_type == "select_dropdown":
            blocks.append(SelectDropdownNavBlock())

        # Ensure full fallback chain, no duplicates
        existing_types = {type(b) for b in blocks}
        for cls in (AnchorTextNavBlock, SlugIncrementNavBlock, FanficNavBlock, AINavBlock):
            if cls not in existing_types:
                blocks.append(cls())
        return blocks

    def _validate_blocks(self) -> list[ScraperBlock]:
        from pipeline.validator import LengthValidatorBlock, ProseRichnessBlock
        return [LengthValidatorBlock(min_chars=100), ProseRichnessBlock(min_word_count=20)]

    # ── Runner ─────────────────────────────────────────────────────────────────

    @classmethod
    def from_profile(cls, profile: dict) -> "PipelineRunner":
        """Tạo runner từ profile dict. Luôn thành công — không còn trả về None."""
        return cls(profile)

    @classmethod
    def default(cls, domain: str = "") -> "PipelineRunner":
        """Runner mặc định với empty profile — chỉ dùng heuristics."""
        return cls({})

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
            fetch_result = await ChainExecutor(self._fetch_blocks(), "fetch").run(ctx)
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
        extract_result = await ChainExecutor(self._extract_blocks(), "extract").run(ctx)
        if extract_result.ok:
            ctx.content       = extract_result.data
            ctx.selector_used = extract_result.metadata.get("selector")
            from utils.content_cleaner import clean_extracted_content
            cleaned = clean_extracted_content(ctx.content)
            if cleaned != ctx.content:
                logger.debug(
                    "[Runner] content_cleaner: %d→%d chars for %s",
                    len(ctx.content), len(cleaned), url[:55],
                )
            ctx.content = cleaned

        # 4. Extract title (weighted vote — all blocks run)
        title_result = await ChainExecutor(
            self._title_blocks(), "title", special_mode="title_vote"
        ).run(ctx)
        if title_result.ok:
            ctx.title_clean = title_result.data
            ctx.title_raw   = title_result.data

        # 5. Navigate
        nav_result = await ChainExecutor(self._nav_blocks(), "navigate").run(ctx)
        if nav_result.ok:
            ctx.next_url   = nav_result.data
            ctx.nav_method = nav_result.method_used

        # 6. Validate
        await ChainExecutor(self._validate_blocks(), "validate").run(ctx)

        return ctx


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
    runner = PipelineRunner.from_profile(profile)
    return await runner.run(
        url             = url,
        profile         = profile,
        progress        = progress,
        pool            = pool,
        pw_pool         = pw_pool,
        ai_limiter      = ai_limiter,
        prefetched_html = prefetched_html,
    )