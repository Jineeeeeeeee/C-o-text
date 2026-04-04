"""
core/session_pool.py — DomainSessionPool (curl_cffi) + PlaywrightPool.

Cải tiến từ phiên bản cũ:
  - PlaywrightPool: periodic restart giải phóng RAM (mỗi PW_RESTART_EVERY fetch)
  - DomainSessionPool: tự động mark CF domain và shortcut sang Playwright
"""
from __future__ import annotations

import asyncio

from config import (
    CHROME_UA, pick_chrome_version, make_headers, REQUEST_TIMEOUT,
)
from utils.string_helpers import CF_CHALLENGE_TITLES

PW_RESTART_EVERY = 300  # Restart Playwright sau N fetch để giải phóng RAM

_CRASH_SIGNALS = (
    "Connection closed", "Browser.new_context",
    "Protocol error", "Target closed", "browser has disconnected",
)


# ── DomainSessionPool ─────────────────────────────────────────────────────────

class DomainSessionPool:
    """1 curl_cffi session per domain. CF-flagged domains → Playwright."""

    def __init__(self) -> None:
        self._sessions:   dict[str, object] = {}
        self._versions:   dict[str, str]    = {}
        self._cf_domains: set[str]          = set()
        self._lock = asyncio.Lock()

    def is_cf_domain(self, domain: str) -> bool:
        return domain in self._cf_domains

    def mark_cf_domain(self, domain: str) -> None:
        self._cf_domains.add(domain)

    async def _get_session(self, domain: str):
        async with self._lock:
            if domain not in self._sessions:
                try:
                    from curl_cffi.requests import AsyncSession
                except ImportError:
                    raise ImportError("curl_cffi chưa cài:\n  pip install curl_cffi")
                ver = pick_chrome_version()
                self._versions[domain]  = ver
                self._sessions[domain]  = AsyncSession(impersonate=ver)
            return self._sessions[domain], self._versions[domain]

    async def fetch(self, url: str) -> tuple[int, str]:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        session, ver = await self._get_session(domain)
        headers = make_headers(ver)
        resp = await session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        return resp.status_code, resp.text

    async def close_all(self) -> None:
        async with self._lock:
            for s in self._sessions.values():
                try:
                    await s.close()
                except Exception:
                    pass
            self._sessions.clear()
            self._versions.clear()


# ── PlaywrightPool ────────────────────────────────────────────────────────────

class PlaywrightPool:
    """Singleton Playwright browser — khởi động 1 lần, tái dùng suốt phiên."""

    def __init__(self) -> None:
        self._pw          = None
        self._browser     = None
        self._stealth     = None
        self._lock        = asyncio.Lock()
        self._started     = False
        self._fetch_count = 0

    async def _maybe_restart(self) -> None:
        if self._fetch_count > 0 and self._fetch_count % PW_RESTART_EVERY == 0:
            print(
                f"  [Browser] 🔄 Periodic restart sau {self._fetch_count} fetch...",
                flush=True,
            )
            async with self._lock:
                await self._cleanup()

    async def _ensure_started(self) -> None:
        if self._started and self._browser and self._browser.is_connected():
            return
        async with self._lock:
            if self._started and self._browser and self._browser.is_connected():
                return
            if self._started or self._browser:
                await self._cleanup()
            await self._start()

    async def _cleanup(self) -> None:
        for obj in (self._browser, self._pw):
            try:
                if obj:
                    await obj.close() if hasattr(obj, 'close') else await obj.stop()
            except Exception:
                pass
        self._browser = self._pw = None
        self._started = False

    async def _start(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Playwright chưa cài:\n"
                "  pip install playwright playwright-stealth\n"
                "  playwright install chromium"
            )
        self._pw      = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        try:
            from playwright_stealth import stealth_async
            self._stealth = stealth_async
        except ImportError:
            self._stealth = None
        self._started = True
        print("  [Browser] ✅ Playwright sẵn sàng.", flush=True)

    async def fetch(self, url: str) -> tuple[int, str]:
        await self._maybe_restart()
        for attempt in range(2):
            try:
                await self._ensure_started()
                result = await self._fetch_once(url)
                self._fetch_count += 1
                return result
            except Exception as e:
                if attempt == 0 and any(s in str(e) for s in _CRASH_SIGNALS):
                    async with self._lock:
                        await self._cleanup()
                    continue
                raise
        raise RuntimeError("PlaywrightPool.fetch: unexpected exit")

    async def close(self) -> None:          
        """Public shutdown — gọi từ AppState.close()."""
        async with self._lock:
            await self._cleanup()

    async def _fetch_once(self, url: str) -> tuple[int, str]:
        context = await self._browser.new_context(
            user_agent         = CHROME_UA["chrome124"],
            viewport           = {"width": 1280, "height": 800},
            locale             = "en-US",
            timezone_id        = "America/New_York",
            extra_http_headers = {
                "Accept-Language": "en-US,en;q=0.9",
                "Accept"         : "text/html,application/xhtml+xml,*/*;q=0.8",
            },
        )
        page = await context.new_page()
        try:
            if self._stealth:
                await self._stealth(page)
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # Chờ CF challenge timeout
            # FIX: wrap page.title() trong try/except — nếu page navigate trong lúc chờ
            # thì "Execution context was destroyed" sẽ được bắt và vòng lặp dừng lại
            for _ in range(20):
                try:
                    title = (await page.title()).strip().lower()
                except Exception:
                    break   # Page đã navigate xong, CF cleared
                if title not in CF_CHALLENGE_TITLES:
                    break
                await page.wait_for_timeout(1_000)

            html   = await page.content()
            status = resp.status if resp else 200
            return status, html
        finally:
            try:
                await context.close()
            except Exception:
                pass