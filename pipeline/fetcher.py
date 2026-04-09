"""
pipeline/fetcher.py — Fetch blocks.

v2 changes:
  FETCH-1: Tất cả blocks đọc pool/pw_pool từ ctx.runtime thay vì
           ctx.profile.get("_pool") (anti-pattern cũ).

  FETCH-2: HybridFetchBlock._detect_js_fetch() KHÔNG còn mutate
           ctx.profile["requires_playwright"] = True.
           Thay vào đó: trả về BlockResult với metadata{"js_heavy": True}.
           Executor đọc signal này, set ctx.detected_js_heavy = True.
           Caller (scraper.py) quyết định có persist xuống profile không.

  P1-B: _JS_CONTENT_RATIO và _JS_MIN_DIFF_CHARS được import từ config.py
        thay vì hardcode tại đây. Một source of truth cho cả project.

Blocks:
    CurlFetchBlock       — curl_cffi Chrome TLS fingerprint (nhanh, ít RAM)
    PlaywrightFetchBlock — Playwright full browser (JS support)
    HybridFetchBlock     — Thử curl trước, auto-fallback Playwright nếu CF.
                           detect_js=True (learning mode): fetch cả 2, so sánh.
"""
from __future__ import annotations

import asyncio
import time

from config import JS_CONTENT_RATIO as _JS_CONTENT_RATIO, JS_MIN_DIFF_CHARS as _JS_MIN_DIFF_CHARS
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
                ),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e).strip() or repr(e), method_used="curl"), start)

    def to_config(self) -> dict:
        return {"type": self.name}

    @classmethod
    def from_config(cls, config: dict) -> "CurlFetchBlock":
        return cls()


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
                return self._timed(
                    BlockResult.failed(f"junk_page status={status}"),
                    start,
                )

            return self._timed(
                BlockResult.success(
                    data        = html,
                    method_used = "playwright",
                    confidence  = 1.0,
                    char_count  = len(html),
                ),
                start,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._timed(BlockResult.failed(str(e).strip() or repr(e), method_used="playwright"), start)

    def to_config(self) -> dict:
        return {"type": self.name}

    @classmethod
    def from_config(cls, config: dict) -> "PlaywrightFetchBlock":
        return cls()


class HybridFetchBlock(ScraperBlock):
    """
    Smart fetch: curl first, auto-fallback Playwright khi cần.

    Runtime flow (đã có profile):
        1. profile.requires_playwright = True → Playwright thẳng
        2. Domain đã flagged CF             → Playwright thẳng
        3. Thử curl → CF detected           → Playwright, flag domain
        4. curl thành công                  → trả về curl result

    Learning mode (detect_js=True):
        1. Fetch bằng CẢ curl VÀ Playwright
        2. So sánh text content length
        3. Nếu Playwright > curl × JS_CONTENT_RATIO AND diff > JS_MIN_DIFF_CHARS:
           → Báo "js_heavy": True qua BlockResult.metadata
           → Executor set ctx.detected_js_heavy = True
           → Caller persist vào profile NẾU phù hợp
           KHÔNG tự mutate ctx.profile — đó không phải việc của block.

    P1-B: threshold _JS_CONTENT_RATIO, _JS_MIN_DIFF_CHARS import từ config.py.
    """
    block_type = BlockType.FETCH
    name       = "hybrid"

    def __init__(self, detect_js: bool = False) -> None:
        self.detect_js = detect_js

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
                    ),
                    start,
                )

            # Learning mode: detect JS-heavy
            if self.detect_js:
                return self._timed(
                    await self._detect_js_fetch(ctx, pool, pw_pool, domain),
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

    async def _detect_js_fetch(
        self,
        ctx   : PipelineContext,
        pool,
        pw_pool,
        domain: str,
    ) -> BlockResult:
        """
        Fetch bằng cả curl và Playwright để detect JS-heavy site.

        Signal "js_heavy" được trả về trong BlockResult.metadata.
        KHÔNG mutate ctx.profile — đó là việc của executor/caller.

        P1-B: dùng _JS_CONTENT_RATIO, _JS_MIN_DIFF_CHARS từ config.py.
        """
        from bs4 import BeautifulSoup

        curl_html = pw_html = ""
        curl_ok   = pw_ok   = False

        try:
            _, curl_html = await pool.fetch(ctx.url)
            curl_ok = (
                not is_cloudflare_challenge(curl_html)
                and not is_junk_page(curl_html)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

        try:
            _, pw_html = await pw_pool.fetch(ctx.url)
            pw_ok = not is_junk_page(pw_html)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

        if not pw_ok and not curl_ok:
            return BlockResult.failed("both curl and playwright failed")

        def _text_len(html: str) -> int:
            if not html:
                return 0
            try:
                return len(BeautifulSoup(html, "html.parser").get_text())
            except Exception:
                return len(html)

        curl_len = _text_len(curl_html) if curl_ok else 0
        pw_len   = _text_len(pw_html)   if pw_ok   else 0

        is_js_heavy = (
            pw_ok and curl_ok
            and pw_len > curl_len * _JS_CONTENT_RATIO
            and (pw_len - curl_len) > _JS_MIN_DIFF_CHARS
        )

        if is_js_heavy:
            print(
                f"  [Hybrid] 🔍 JS-heavy detected on {domain}: "
                f"curl={curl_len:,}c vs pw={pw_len:,}c "
                f"(ratio={pw_len/max(curl_len,1):.1f}x)",
                flush=True,
            )

        if curl_ok and is_cloudflare_challenge(curl_html):
            pool.mark_cf_domain(domain)

        best_html   = pw_html   if pw_ok   else curl_html
        best_method = "playwright" if pw_ok else "curl"

        return BlockResult.success(
            data        = best_html,
            method_used = f"hybrid_detect_{best_method}",
            confidence  = 1.0,
            char_count  = len(best_html),
            js_heavy    = is_js_heavy,
            curl_len    = curl_len,
            pw_len      = pw_len,
        )

    def to_config(self) -> dict:
        return {"type": self.name, "detect_js": self.detect_js}

    @classmethod
    def from_config(cls, config: dict) -> "HybridFetchBlock":
        return cls(detect_js=bool(config.get("detect_js", False)))


# ── Internal sentinel ──────────────────────────────────────────────────────────

class _CloudflareError(Exception):
    pass


# ── Registry ───────────────────────────────────────────────────────────────────

_FETCH_BLOCK_MAP: dict[str, type[ScraperBlock]] = {
    "curl"      : CurlFetchBlock,
    "playwright": PlaywrightFetchBlock,
    "hybrid"    : HybridFetchBlock,
}


def make_fetch_block(config: dict) -> ScraperBlock:
    block_type = config.get("type", "hybrid")
    cls = _FETCH_BLOCK_MAP.get(block_type)
    if cls is None:
        raise ValueError(
            f"Unknown fetch block type: {block_type!r}. "
            f"Available: {list(_FETCH_BLOCK_MAP)}"
        )
    return cls.from_config(config)