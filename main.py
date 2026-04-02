"""
main.py — Entry point duy nhất của Cào Text.

Flow:
  1. Đọc links.txt
  2. Khởi tạo AppState (pools, rate limiter, profile manager)
  3. Stagger tasks → asyncio.gather
  4. Graceful shutdown khi Ctrl+C
"""
import sys
import io
import asyncio
import hashlib
import os
from datetime import datetime
from urllib.parse import urlparse

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

from config import INIT_STAGGER, AI_MAX_RPM, OUTPUT_DIR, PROGRESS_DIR
from ai.client             import AIRateLimiter
from core.session_pool     import DomainSessionPool, PlaywrightPool
from core.scraper          import run_novel_task
from learning.profile_manager import ProfileManager
from utils.file_io         import load_profiles, ensure_dirs


# ── AppState ──────────────────────────────────────────────────────────────────

class AppState:
    __slots__ = (
        "profiles_lock", "total_lock",
        "ai_limiter", "pw_pool",
        "_total", "_start_time",
    )

    def __init__(self) -> None:
        self.profiles_lock = asyncio.Lock()
        self.total_lock    = asyncio.Lock()
        self.ai_limiter    = AIRateLimiter(AI_MAX_RPM)
        self.pw_pool       = PlaywrightPool()
        self._total        = 0
        self._start_time   = datetime.now()

    @property
    def total(self) -> int:
        return self._total

    async def inc_total(self) -> int:
        async with self.total_lock:
            self._total += 1
            return self._total

    def elapsed(self) -> str:
        s = int((datetime.now() - self._start_time).total_seconds())
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    async def close(self) -> None:
        await self.pw_pool.close()


# ── URL helpers ───────────────────────────────────────────────────────────────

def _valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def _output_dir(url: str) -> str:
    p      = urlparse(url)
    domain = p.netloc.replace("www.", "").replace(".", "_")
    parts  = [seg for seg in p.path.strip("/").split("/") if seg][:2]
    slug   = "_".join(parts) if parts else "unknown"
    return os.path.join(OUTPUT_DIR, f"{domain}_{slug}")


def _progress_path(url: str) -> str:
    """Progress file path — hash URL 8 chars để tránh collision."""
    out_dir   = _output_dir(url)
    domain    = urlparse(url).netloc.replace(".", "_")
    url_hash  = hashlib.md5(url.encode()).hexdigest()[:8]
    base_slug = out_dir.split(os.sep)[-1]
    return os.path.join(PROGRESS_DIR, f"{domain}_{base_slug}_{url_hash}.json")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    ensure_dirs()

    links_file = sys.argv[1] if len(sys.argv) > 1 else "links.txt"
    if not os.path.exists(links_file):
        print(f"[ERR] Không tìm thấy {links_file}")
        return

    with open(links_file, "r", encoding="utf-8") as f:
        raw_urls = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]

    urls    = [u for u in raw_urls if _valid_url(u)]
    skipped = len(raw_urls) - len(urls)
    if skipped:
        print(f"[WARN] Bỏ qua {skipped} URL không hợp lệ")
    if not urls:
        print("[ERR] Không có URL hợp lệ nào trong links.txt")
        return

    print(f"📚 {len(urls)} truyện cần cào\n")

    app      = AppState()
    pool     = DomainSessionPool()
    profiles = await load_profiles()
    pm       = ProfileManager(profiles, app.profiles_lock)

    print(f"📋 {len(profiles)} domain profile đã load\n")

    async def _task(url: str, idx: int) -> None:
        await asyncio.sleep(idx * INIT_STAGGER)
        await run_novel_task(
            start_url       = url,
            output_dir      = _output_dir(url),
            progress_path   = _progress_path(url),
            pool            = pool,
            pw_pool         = app.pw_pool,
            pm              = pm,
            ai_limiter      = app.ai_limiter,
            on_chapter_done = app.inc_total,
        )

    cancelled = False
    try:
        results = await asyncio.gather(
            *[_task(url, i) for i, url in enumerate(urls)],
            return_exceptions=True,
        )
    except asyncio.CancelledError:
        cancelled = True
        print("\n⚠️  Nhận tín hiệu dừng (Ctrl+C). Progress đã lưu.", flush=True)
        results = []
    finally:
        await pool.close_all()
        await app.close()

    if not cancelled:
        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                print(
                    f"[ERR] {url[:60]}\n"
                    f"      {type(result).__name__}: {result}",
                    flush=True,
                )

    print(
        f"\n{'─'*60}\n"
        f"✔ Tổng kết: {app.total} chapters trong {app.elapsed()}\n"
        f"{'─'*60}"
    )


if __name__ == "__main__":
    asyncio.run(main())