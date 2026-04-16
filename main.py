"""
main.py — Entry point duy nhất của Cào Text.

v3: Pipeline Architecture.
  CLI-1: Thêm --max-pw-instances N (default=2, override PW_MAX_CONCURRENCY)
  CLI-2: Thêm --fast-learning (bỏ qua optimizer, chỉ dùng AI selectors)
  CLI-3: Thêm --no-validation (bỏ qua ProseRichnessBlock — nhanh hơn nhưng ít filter)
  RELEARN-1: Giữ nguyên `!relearn <domain>` trong links.txt.
  ISSUE-1:   Gọi write_session_header() khi bắt đầu.
"""
import sys
import io
import asyncio
import argparse
import hashlib
import os
from datetime import datetime
from urllib.parse import urlparse

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)

import config as _cfg   # import trước để có thể override constants
from config import INIT_STAGGER, AI_MAX_RPM, OUTPUT_DIR, PROGRESS_DIR
from ai.client                import AIRateLimiter
from core.session_pool        import DomainSessionPool, PlaywrightPool
from core.scraper             import run_novel_task
from learning.profile_manager import ProfileManager
from utils.file_io            import load_profiles, save_profiles, ensure_dirs
from utils.issue_reporter     import write_session_header


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
    out_dir   = _output_dir(url)
    domain    = urlparse(url).netloc.replace(".", "_")
    dir_hash  = hashlib.md5(out_dir.encode()).hexdigest()[:8]
    base_slug = out_dir.split(os.sep)[-1]
    return os.path.join(PROGRESS_DIR, f"{domain}_{base_slug}_{dir_hash}.json")


# ── links.txt parser ──────────────────────────────────────────────────────────

def _parse_links_file(path: str) -> tuple[list[str], list[str]]:
    """
    Parse links.txt. Trả về (urls, relearn_domains).

    Hỗ trợ:
      - URL hợp lệ
      - `# comment`: bỏ qua
      - `!relearn <domain>`: xóa profile domain này → force re-learn
    """
    urls            : list[str] = []
    relearn_domains : list[str] = []

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("!relearn"):
                parts = line.split(None, 1)
                if len(parts) == 2:
                    domain = parts[1].strip().lower()
                    if domain:
                        relearn_domains.append(domain)
                        print(f"  [Config] 🔄 Force re-learn: {domain}", flush=True)
                else:
                    print(f"  [Config] ⚠ Bỏ qua dòng !relearn không hợp lệ: {line!r}", flush=True)
                continue
            if _valid_url(line):
                urls.append(line)

    return urls, relearn_domains


async def _apply_relearn(
    relearn_domains : list[str],
    profiles        : dict,
    profiles_lock   : asyncio.Lock,
) -> int:
    if not relearn_domains:
        return 0
    removed = 0
    async with profiles_lock:
        for domain in relearn_domains:
            to_delete = [
                k for k in profiles
                if k == domain or k == f"www.{domain}" or f"www.{domain}" == k
            ]
            for key in to_delete:
                del profiles[key]
                print(f"  [Config] 🗑  Profile '{key}' đã xóa → sẽ re-learn", flush=True)
                removed += 1
        if removed > 0:
            await save_profiles(profiles)
    return removed


# ── CLI argument parser ───────────────────────────────────────────────────────

def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog        = "main.py",
        description = "Cào Text — Web novel scraper với Pipeline Architecture",
    )
    parser.add_argument(
        "links_file",
        nargs   = "?",
        default = "links.txt",
        help    = "File chứa URLs cần cào (default: links.txt)",
    )
    parser.add_argument(
        "--max-pw-instances",
        type    = int,
        default = None,
        metavar = "N",
        help    = f"Số Playwright instances tối đa (default: {_cfg.PW_MAX_CONCURRENCY})",
    )
    parser.add_argument(
        "--fast-learning",
        action  = "store_true",
        help    = "Bỏ qua optimizer, chỉ dùng AI selectors (nhanh hơn ~30%%)",
    )
    parser.add_argument(
        "--no-validation",
        action  = "store_true",
        help    = "Bỏ qua ProseRichnessBlock (ít filter hơn, nhanh hơn nhẹ)",
    )
    return parser


def _apply_cli_overrides(args: argparse.Namespace) -> None:
    """Apply CLI args vào config module (global override)."""
    if args.max_pw_instances is not None:
        _cfg.PW_MAX_CONCURRENCY = args.max_pw_instances
        print(f"  [Config] PW_MAX_CONCURRENCY = {_cfg.PW_MAX_CONCURRENCY}", flush=True)

    if args.fast_learning:
        # Flag được đọc bởi learning/phase.py
        os.environ["CAO_FAST_LEARNING"] = "1"
        print(f"  [Config] Fast learning mode: optimizer disabled", flush=True)

    if args.no_validation:
        os.environ["CAO_NO_VALIDATION"] = "1"
        print(f"  [Config] Validation: ProseRichnessBlock disabled", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    ensure_dirs()

    # Parse CLI
    parser = _build_arg_parser()
    # Tách links_file ra để không conflict với positional args
    args = parser.parse_args()
    _apply_cli_overrides(args)

    links_file = args.links_file
    if not os.path.exists(links_file):
        print(f"[ERR] Không tìm thấy {links_file}")
        return

    urls, relearn_domains = _parse_links_file(links_file)

    # Đếm dòng không hợp lệ
    skipped_invalid = 0
    with open(links_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.lower().startswith("!relearn"):
                continue
            if not _valid_url(line):
                skipped_invalid += 1

    if skipped_invalid:
        print(f"[WARN] Bỏ qua {skipped_invalid} URL không hợp lệ")
    if not urls:
        print("[ERR] Không có URL hợp lệ nào trong links.txt")
        return

    print(f"📚 {len(urls)} truyện cần cào\n")

    app      = AppState()
    pool     = DomainSessionPool()
    profiles = await load_profiles()
    pm       = ProfileManager(profiles, app.profiles_lock)

    # Apply Playwright semaphore limit
    if _cfg.PW_MAX_CONCURRENCY > 0:
        import asyncio as _aio
        app.pw_pool._semaphore = _aio.Semaphore(_cfg.PW_MAX_CONCURRENCY)

    # RELEARN-1: Xóa profiles của domain được yêu cầu re-learn
    if relearn_domains:
        removed = await _apply_relearn(relearn_domains, profiles, app.profiles_lock)
        if removed == 0:
            print(f"  [Config] ℹ️  Không tìm thấy profile nào để xóa cho {relearn_domains}", flush=True)

    print(f"📋 {len(profiles)} domain profile đã load\n")
    write_session_header(len(urls))

    # ── Phase 1: Sequential learning ──────────────────────────────────────────────
    # Học tất cả domains cần học TRƯỚC, tuần tự từng domain một.
    # Phase 2 (scraping) sẽ tìm thấy profiles và không học lại → không bao giờ
    # học + cào đồng thời.
    from core.scraper import run_learning_only

    print(f"🎓 Phase 1: Kiểm tra và học {len(urls)} domain(s)...\n", flush=True)
    seen_learning: set[str] = set()
    for url in urls:
        domain = urlparse(url).netloc.lower()
        if domain in seen_learning:
            continue
        seen_learning.add(domain)
        try:
            await run_learning_only(
                start_url     = url,
                progress_path = _progress_path(url),
                pool          = pool,
                pw_pool       = app.pw_pool,
                pm            = pm,
                ai_limiter    = app.ai_limiter,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [WARN] Learning thất bại cho {url[:55]}: {e}", flush=True)

    print(f"\n🚀 Phase 2: Bắt đầu cào {len(urls)} truyện...\n", flush=True)

    # ── Phase 2: Concurrent scraping ───────────────────────────────────────────────
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
    # Suppress Windows asyncio cleanup noise ("I/O operation on closed pipe"
    # xuất hiện khi ProactorEventLoop dọn dẹp transport lúc thoát chương trình).
    if sys.platform == "win32":
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)

        def _silence_transport_errors(loop, context):
            msg = context.get("message", "")
            exc = context.get("exception")
            exc_str = str(exc) if exc else ""
            if any(kw in msg.lower() or kw in exc_str.lower() for kw in (
                "i/o operation on closed",
                "transport",
                "pipe",
            )):
                return  # suppress silently
            loop.default_exception_handler(context)

        _loop.set_exception_handler(_silence_transport_errors)
        try:
            _loop.run_until_complete(main())
        finally:
            _loop.close()
    else:
        asyncio.run(main())