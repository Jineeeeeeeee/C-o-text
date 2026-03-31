"""
core/session_pool.py — Quản lý HTTP sessions và Playwright browser pool.

FIX-PW1: PlaywrightPool._ensure_started() kiểm tra browser.is_connected() —
          nếu browser crash thì tự restart thay vì dùng zombie instance.

FIX-PW2: PlaywrightPool.fetch() retry 1 lần khi gặp browser crash errors
          ("Connection closed", "Protocol error", "Browser.new_context").

FIX-PW3: context.close() trong finally bắt Exception riêng —
          không để lỗi "Failed to find context" làm crash toàn bộ task.
"""
import asyncio

from config import CHROME_UA, pick_chrome_version, make_headers, REQUEST_TIMEOUT
from utils.string_helpers import CF_CHALLENGE_TITLES


# ── DomainSessionPool ─────────────────────────────────────────────────────────

class DomainSessionPool:
    """
    Pool curl_cffi session — 1 session/domain.
    Domain từng trigger CF challenge sẽ được chuyển thẳng sang Playwright.
    """

    def __init__(self) -> None:
        self._sessions:  dict[str, object] = {}
        self._versions:  dict[str, str]    = {}
        self._cf_domains: set[str]          = set()
        self._lock = asyncio.Lock()

    def mark_cf_domain(self, domain: str) -> None:
        self._cf_domains.add(domain)

    def is_cf_domain(self, domain: str) -> bool:
        return domain in self._cf_domains

    async def _get_session(self, domain: str):
        async with self._lock:
            if domain not in self._sessions:
                try:
                    from curl_cffi.requests import AsyncSession
                except ImportError:
                    raise ImportError("curl_cffi chưa cài:\n  pip install curl_cffi")
                version = pick_chrome_version()
                self._versions[domain] = version
                self._sessions[domain] = AsyncSession(impersonate=version)
            return self._sessions[domain], self._versions[domain]

    async def fetch(self, url: str) -> tuple[int, str]:
        from urllib.parse import urlparse
        domain           = urlparse(url).netloc.lower()
        session, version = await self._get_session(domain)
        headers          = make_headers(version)
        resp             = await session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        return resp.status_code, resp.text

    async def close_all(self) -> None:
        async with self._lock:
            for session in self._sessions.values():
                try:
                    await session.close()
                except Exception:
                    pass
            self._sessions.clear()
            self._versions.clear()


# ── PlaywrightPool ────────────────────────────────────────────────────────────

# Lỗi browser crash — cần restart browser
_BROWSER_CRASH_SIGNALS = (
    "Connection closed",
    "Browser.new_context",
    "Protocol error",
    "Target closed",
    "browser has disconnected",
)


class PlaywrightPool:
    """
    Singleton Playwright browser — khởi động 1 lần, tái dùng suốt phiên.

    FIX-PW1: _ensure_started() kiểm tra browser.is_connected() trước khi
             trả về. Nếu browser crash (không còn connected), tự động cleanup
             và restart — tránh dùng zombie browser instance.

    FIX-PW2: fetch() retry tối đa 1 lần khi gặp browser crash error.
             Trước khi retry: reset self._started = False → _ensure_started()
             sẽ tạo lại browser mới.

    FIX-PW3: context.close() được bọc try/except riêng trong finally —
             lỗi "Failed to find context" (Protocol error) không còn làm
             crash task đang chạy.
    """

    def __init__(self) -> None:
        self._pw      = None
        self._browser = None
        self._stealth = None
        self._lock    = asyncio.Lock()
        self._started = False

    # ── FIX-PW1: kiểm tra is_connected() ──────────────────────────────────────
    async def _ensure_started(self) -> None:
        """
        Đảm bảo browser đang chạy và connected.

        Fast path (không cần lock): started AND browser.is_connected()
        Slow path (acquire lock)  : khởi động mới hoặc restart sau crash

        Double-checked locking để tránh race condition khi nhiều task
        đồng thời phát hiện browser chết và cùng gọi _ensure_started().
        """
        # Fast path — tránh acquire lock nếu không cần thiết
        if self._started and self._browser is not None and self._browser.is_connected():
            return

        async with self._lock:
            # Re-check bên trong lock (double-checked locking)
            if self._started and self._browser is not None and self._browser.is_connected():
                return

            # Cleanup stale browser nếu đã từng start
            if self._started or self._browser is not None:
                print("  [Browser] 🔄 Phát hiện browser crash, đang restart...", flush=True)
                await self._cleanup_unsafe()

            # Khởi động browser mới
            await self._start_browser()

    async def _cleanup_unsafe(self) -> None:
        """Dọn dẹp browser cũ — gọi BÊN TRONG lock, không acquire thêm."""
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self._browser = None
        self._pw      = None
        self._started = False

    async def _start_browser(self) -> None:
        """Khởi động Playwright + Chromium — gọi BÊN TRONG lock."""
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
                "--no-sandbox",
                "--disable-setuid-sandbox",
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
        print("  [Browser] ✅ Playwright browser sẵn sàng.", flush=True)

    # ── FIX-PW2 + FIX-PW3: fetch với retry và safe context.close() ───────────
    async def fetch(self, url: str) -> tuple[int, str]:
        """
        Fetch URL bằng Playwright với browser crash recovery.

        Retry flow:
          attempt 0 → fetch bình thường
          Nếu gặp crash error → reset _started → attempt 1 → _ensure_started()
          tạo browser mới → fetch lại
          Nếu attempt 1 vẫn lỗi → raise (không retry vô tận)
        """
        max_attempts = 2

        for attempt in range(max_attempts):
            try:
                await self._ensure_started()
                return await self._fetch_once(url)

            except Exception as e:
                err_str = str(e)
                is_crash = any(sig in err_str for sig in _BROWSER_CRASH_SIGNALS)

                if attempt < max_attempts - 1 and is_crash:
                    print(
                        f"  [Browser] ⚠️  Browser lỗi (lần {attempt + 1}): {err_str[:80]}",
                        flush=True,
                    )
                    # Force restart: reset flag trước khi retry
                    async with self._lock:
                        await self._cleanup_unsafe()
                    continue

                raise  # Không phải crash error, hoặc đã retry hết lần

        # Unreachable — loop luôn return hoặc raise
        raise RuntimeError("PlaywrightPool.fetch: retry logic không mong đợi")

    async def _fetch_once(self, url: str) -> tuple[int, str]:
        """
        Một lần fetch thực sự — tạo context, navigate, lấy HTML, đóng context.

        FIX-PW3: context.close() bọc try/except riêng để lỗi
        "Failed to find context" (xảy ra khi browser crash giữa chừng)
        không làm mất exception gốc.
        """
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

            # Chờ CF challenge tự giải (tối đa 20s)
            for _ in range(20):
                title = (await page.title()).strip().lower()
                if title not in CF_CHALLENGE_TITLES:
                    break
                await page.wait_for_timeout(1_000)
            else:
                print("  [Browser] ⚠️  CF vẫn còn sau 20s.", flush=True)

            html   = await page.content()
            status = resp.status if resp else 200
            return status, html

        finally:
            # FIX-PW3: đóng context nhưng không để lỗi close() lan ra ngoài
            try:
                await context.close()
            except Exception as close_err:
                # "Failed to find context" xảy ra khi browser crash giữa chừng
                # Log nhẹ để biết nhưng không crash task
                print(
                    f"  [Browser] ⚠️  context.close() warning (bỏ qua): "
                    f"{str(close_err)[:60]}",
                    flush=True,
                )

    async def close(self) -> None:
        if not self._started:
            return
        async with self._lock:
            await self._cleanup_unsafe()