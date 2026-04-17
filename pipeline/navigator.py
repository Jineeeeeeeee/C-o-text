"""
pipeline/navigator.py — Navigation blocks.

Batch B: Xóa to_config(), from_config(), make_nav_block(), registry dict.
  Blocks được instantiate trực tiếp bởi PipelineRunner._nav_blocks().

Blocks (theo thứ tự ưu tiên):
    RelNextNavBlock       — <link rel="next"> / <a rel="next">  (chuẩn nhất)
    SelectorNavBlock      — CSS selector từ profile
    AnchorTextNavBlock    — Text "Next", "Next Chapter", v.v.
    SlugIncrementNavBlock — /chapter-5 → /chapter-6
    FanficNavBlock        — fanfiction.net /s/{id}/{num}/
    SelectDropdownNavBlock — <select> chapter dropdown
    AINavBlock            — AI fallback (tốn API call, chỉ dùng khi mọi thứ fail)
"""
from __future__ import annotations

import asyncio
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from config import RE_NEXT_BTN, RE_CHAP_SLUG, RE_FANFIC
from pipeline.base import BlockType, BlockResult, PipelineContext, ScraperBlock


# ── 1. Rel Next ───────────────────────────────────────────────────────────────

class RelNextNavBlock(ScraperBlock):
    """
    Tìm URL tiếp theo qua HTML rel="next".
    Chuẩn SEO — confidence cao nhất vì site tự declare link.
    """
    block_type = BlockType.NAVIGATE
    name       = "rel_next"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            soup = ctx.soup
            if soup is None:
                return self._timed(BlockResult.skipped("no soup"), start)

            el = soup.find("link", rel="next") or soup.find("a", rel="next")
            if el and el.get("href"):
                url = urljoin(ctx.url, el["href"])
                return self._timed(
                    BlockResult.success(
                        data        = url,
                        method_used = "rel_next",
                        confidence  = 0.98,
                    ),
                    start,
                )
            return self._timed(BlockResult.failed("no rel=next found"), start)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)


# ── 2. Selector Nav ───────────────────────────────────────────────────────────

class SelectorNavBlock(ScraperBlock):
    """CSS selector đã học từ profile."""
    block_type = BlockType.NAVIGATE
    name       = "selector"

    def __init__(self, selector: str | None = None) -> None:
        self.selector = selector

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            sel = self.selector or ctx.profile.get("next_selector")
            if not sel:
                return self._timed(BlockResult.skipped("no next_selector"), start)

            soup = ctx.soup
            if soup is None:
                return self._timed(BlockResult.skipped("no soup"), start)

            el = soup.select_one(sel)
            if el is None:
                return self._timed(
                    BlockResult.failed(f"selector {sel!r} matched nothing"),
                    start,
                )

            href = el.get("href")
            if not href:
                inner = el.find("a", href=True)
                href  = inner.get("href") if inner else None

            if not href:
                return self._timed(
                    BlockResult.failed(f"selector {sel!r}: no href"),
                    start,
                )

            url = urljoin(ctx.url, href)
            return self._timed(
                BlockResult.success(
                    data        = url,
                    method_used = f"selector:{sel}",
                    confidence  = 0.92,
                ),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)


# ── 3. Anchor Text Nav ────────────────────────────────────────────────────────

class AnchorTextNavBlock(ScraperBlock):
    """Tìm link có anchor text khớp RE_NEXT_BTN."""
    block_type = BlockType.NAVIGATE
    name       = "anchor_text"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            soup = ctx.soup
            if soup is None:
                return self._timed(BlockResult.skipped("no soup"), start)

            for a in soup.find_all("a", href=True):
                if RE_NEXT_BTN.search(a.get_text(strip=True)):
                    url = urljoin(ctx.url, a["href"])
                    return self._timed(
                        BlockResult.success(
                            data        = url,
                            method_used = "anchor_text",
                            confidence  = 0.80,
                            anchor_text = a.get_text(strip=True)[:30],
                        ),
                        start,
                    )
            return self._timed(
                BlockResult.failed("no anchor with next-button text"),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)


# ── 4. Slug Increment ─────────────────────────────────────────────────────────

class SlugIncrementNavBlock(ScraperBlock):
    """/chapter-5 → /chapter-6 bằng regex RE_CHAP_SLUG."""
    block_type = BlockType.NAVIGATE
    name       = "slug_increment"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            m = RE_CHAP_SLUG.search(ctx.url)
            if m:
                new_url = f"{m.group(1)}{int(m.group(2)) + 1}{m.group(3)}"
                return self._timed(
                    BlockResult.success(
                        data        = new_url,
                        method_used = "slug_increment",
                        confidence  = 0.70,
                    ),
                    start,
                )
            return self._timed(BlockResult.failed("no slug pattern in URL"), start)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)


# ── 5. Fanfic Nav ─────────────────────────────────────────────────────────────

class FanficNavBlock(ScraperBlock):
    """fanfiction.net /s/{story_id}/{chapter_num}/{title}"""
    block_type = BlockType.NAVIGATE
    name       = "fanfic"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            m = RE_FANFIC.search(ctx.url)
            if m:
                new_url = (
                    ctx.url[: m.start()]
                    + m.group(1)
                    + str(int(m.group(2)) + 1)
                    + (m.group(3) or "")
                )
                return self._timed(
                    BlockResult.success(
                        data        = new_url,
                        method_used = "fanfic_increment",
                        confidence  = 0.72,
                    ),
                    start,
                )
            return self._timed(
                BlockResult.failed("URL does not match fanfic pattern"),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)


# ── 6. Select Dropdown Nav ────────────────────────────────────────────────────

class SelectDropdownNavBlock(ScraperBlock):
    """
    Tìm next chapter từ <select> dropdown.
    Logic: tìm <option selected>, lấy <option> kế tiếp trong DOM.
    Fallback: tìm option có value khớp URL hiện tại.
    """
    block_type = BlockType.NAVIGATE
    name       = "select_dropdown"

    _AUTO_SELECTORS = [
        "select#chapterList",
        "select.chapter-select",
        "select[name='chapter']",
        "select.selectpicker",
        "select#chapter",
        "select.chapter-dropdown",
        "select",
    ]

    def __init__(self, select_selector: str | None = None) -> None:
        self.select_selector = select_selector

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            soup = ctx.soup
            if soup is None:
                return self._timed(BlockResult.skipped("no soup"), start)

            selectors = (
                [self.select_selector] if self.select_selector
                else self._AUTO_SELECTORS
            )

            for sel in selectors:
                try:
                    select_el = soup.select_one(sel)
                    if select_el is None:
                        continue

                    options = select_el.find_all("option")
                    if not options:
                        continue

                    current_idx = None
                    for i, opt in enumerate(options):
                        if opt.get("selected") is not None:
                            current_idx = i
                            break

                    if current_idx is None:
                        for i, opt in enumerate(options):
                            val = opt.get("value", "")
                            if val and val in ctx.url:
                                current_idx = i
                                break

                    if current_idx is None or current_idx >= len(options) - 1:
                        continue

                    next_val = options[current_idx + 1].get("value", "").strip()
                    if not next_val:
                        continue

                    url = urljoin(ctx.url, next_val)
                    return self._timed(
                        BlockResult.success(
                            data            = url,
                            method_used     = f"select_dropdown:{sel}",
                            confidence      = 0.85,
                            select_selector = sel,
                        ),
                        start,
                    )
                except Exception:
                    continue

            return self._timed(BlockResult.failed("no chapter dropdown found"), start)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)


# ── 7. AI Nav Block ───────────────────────────────────────────────────────────

class AINavBlock(ScraperBlock):
    """
    AI fallback navigation.
    Chỉ dùng khi tất cả heuristic blocks thất bại.
    """
    block_type = BlockType.NAVIGATE
    name       = "ai_nav"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            ai_limiter = ctx.runtime.ai_limiter
            if ai_limiter is None:
                return self._timed(
                    BlockResult.skipped("no ai_limiter in runtime"),
                    start,
                )

            html = ctx.html
            if not html:
                return self._timed(BlockResult.skipped("no html"), start)

            from ai.agents import ai_classify_and_find
            result = await ai_classify_and_find(html, ctx.url, ai_limiter)

            if result and result.get("next_url"):
                return self._timed(
                    BlockResult.fallback(
                        data        = result["next_url"],
                        method_used = "ai_nav",
                        confidence  = 0.75,
                    ),
                    start,
                )
            return self._timed(BlockResult.failed("AI could not find next URL"), start)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e) or repr(e)), start)