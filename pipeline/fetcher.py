"""
pipeline/fetcher.py — Fetch blocks.

Batch B: Xóa to_config(), from_config(), make_fetch_block(), registry dict.
  Blocks được instantiate trực tiếp bởi PipelineRunner._fetch_blocks().

Batch B: Xóa detect_js từ HybridFetchBlock.
  JS-heavy detection xảy ra trong learning/phase.py._detect_js_heavy()
  (so sánh curl vs playwright text length), kết quả lưu vào
  profile.requires_playwright. detect_js mode trong block là dead code
  sau khi optimizer bị xóa ở Batch A.

Blocks:
    CurlFetchBlock       — curl_cffi Chrome TLS fingerprint (nhanh, ít RAM)
    PlaywrightFetchBlock — Playwright full browser (JS support)
    HybridFetchBlock     — curl first, auto-fallback Playwright nếu CF
"""
from __future__ import annotations

import asyncio
import time

from utils.string_helpers import is_cloudflare_challenge, is_junk_page
from pipeline.base import (
    BlockType, BlockResult, PipelineContext, ScraperBlock,
)


class CurlFetchBlock(ScraperBlock):
    """
    Fetch bằng curl_cffi Chrome TLS fingerprint.
    Nhanh nhất, ít RAM nhất. Không xử lý JS.
    """
    block_type = BlockType.FETCH
    name       = "curl"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            from urllib.parse import urlparse

            pool   = ctx.runtime.pool
            domain = urlparse(ctx.url).netloc.lower()

            if pool is None:
                return self._timed(BlockResult.failed("DomainSessionPool not in runtime"), start)

            if pool.is_cf_domain(domain):
                return self._timed(
                    BlockResult.skipped("domain flagged as CF — use playwright"),
                    start,
                )

            status, html = await pool.fetch(ctx.url)

            if is_cloudflare_challenge(html):
                pool.mark_cf_domain(domain)
                return self._timed(
                    BlockResult.failed("cloudflare_challenge — domain flagged"),
                    start,
                )

            if is_junk_page(html, status):
                return self._timed(BlockResult.failed(f"junk_page status={status}"), start)

            return self._timed(
                BlockResult.success(
                    data        = html,
                    method_used = "curl",
                    confidence  = 1.0,
                    char_count  = len(html),
                    status_code = status,
                ),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(
                BlockResult.failed(str(e).strip() or repr(e), method_used="curl"),
                start,
            )


class PlaywrightFetchBlock(ScraperBlock):
    """
    Fetch bằng Playwright full browser.
    JS support, bypass một số anti-bot. Chậm hơn curl ~10x, tốn RAM.
    """
    block_type = BlockType.FETCH
    name       = "playwright"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            pw_pool = ctx.runtime.pw_pool

            if pw_pool is None:
                return self._timed(
                    BlockResult.failed("PlaywrightPool not in runtime"),
                    start,
                )

            status, html = await pw_pool.fetch(ctx.url)

            if is_junk_page(html, status):
                return self._timed(BlockResult.failed(f"junk_page status={status}"), start)

            return self._timed(
                BlockResult.success(
                    data        = html,
                    method_used = "playwright",
                    confidence  = 1.0,
                    char_count  = len(html),
                    status_code = status,
                ),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(
                BlockResult.failed(str(e).strip() or repr(e), method_used="playwright"),
                start,
            )


class HybridFetchBlock(ScraperBlock):
    """
    Smart fetch: curl first, auto-fallback Playwright khi cần.

    Runtime flow:
        1. profile.requires_playwright = True → Playwright thẳng
        2. Domain đã flagged CF             → Playwright thẳng
        3. Thử curl → CF detected           → Playwright, flag domain
        4. curl thành công                  → trả về curl result

    Batch B: Bỏ detect_js mode. JS-heavy detection đã được thực hiện
    trong learning/phase.py._detect_js_heavy() và persist vào
    profile.requires_playwright — không cần re-detect mỗi chapter.
    """
    block_type = BlockType.FETCH
    name       = "hybrid"

    async def execute(self, ctx: PipelineContext) -> BlockResult:
        start = time.monotonic()
        try:
            from urllib.parse import urlparse

            pool    = ctx.runtime.pool
            pw_pool = ctx.runtime.pw_pool
            domain  = urlparse(ctx.url).netloc.lower()

            if pool is None or pw_pool is None:
                return self._timed(
                    BlockResult.failed("session pools not in runtime"),
                    start,
                )

            requires_pw = bool(ctx.profile.get("requires_playwright", False))

            # Fast path: known PW-only
            if requires_pw or pool.is_cf_domain(domain):
                status, html = await pw_pool.fetch(ctx.url)
                if is_junk_page(html, status):
                    return self._timed(
                        BlockResult.failed(f"junk_page status={status}"),
                        start,
                    )
                return self._timed(
                    BlockResult.success(
                        data        = html,
                        method_used = "playwright_direct",
                        confidence  = 1.0,
                        char_count  = len(html),
                        status_code = status,
                    ),
                    start,
                )

            # Normal hybrid: curl first, PW fallback
            try:
                status, html = await pool.fetch(ctx.url)
                if is_cloudflare_challenge(html):
                    raise _CloudflareError()
                if is_junk_page(html, status):
                    return self._timed(
                        BlockResult.failed(f"junk_page status={status}"),
                        start,
                    )
                return self._timed(
                    BlockResult.success(
                        data        = html,
                        method_used = "curl",
                        confidence  = 1.0,
                        char_count  = len(html),
                        status_code = status,
                    ),
                    start,
                )

            except _CloudflareError:
                print(f"  [Hybrid] ⚡ CF on {domain} → Playwright", flush=True)
                pool.mark_cf_domain(domain)
                status, html = await pw_pool.fetch(ctx.url)
                if is_junk_page(html, status):
                    return self._timed(
                        BlockResult.failed(f"junk_page status={status} (after CF)"),
                        start,
                    )
                return self._timed(
                    BlockResult.fallback(
                        data        = html,
                        method_used = "playwright_cf_fallback",
                        confidence  = 0.9,
                        status_code = status,
                    ),
                    start,
                )

            except asyncio.CancelledError:
                raise

            except Exception as e:
                err_str = str(e).strip() or repr(e)
                print(f"  [Hybrid] curl error: {err_str[:60]} → Playwright", flush=True)
                try:
                    status, html = await pw_pool.fetch(ctx.url)
                    if is_junk_page(html, status):
                        return self._timed(
                            BlockResult.failed(f"junk_page status={status}"),
                            start,
                        )
                    return self._timed(
                        BlockResult.fallback(
                            data        = html,
                            method_used = "playwright_network_fallback",
                            confidence  = 0.85,
                            status_code = status,
                        ),
                        start,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e2:
                    return self._timed(
                        BlockResult.failed(
                            f"curl: {err_str[:60]} | playwright: {str(e2)[:60]}"
                        ),
                        start,
                    )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e).strip() or repr(e)), start)


# ── Internal sentinel ──────────────────────────────────────────────────────────

class _CloudflareError(Exception):
    pass