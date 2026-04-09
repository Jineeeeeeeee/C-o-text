"""
ai/client.py — Gemini client và AIRateLimiter (token bucket).

FIX-RATELEAK: Rollback timestamp nếu coroutine bị cancel trong jitter sleep.
  Trước: acquire() thêm timestamp vào _timestamps, rồi await asyncio.sleep(jitter).
         Nếu bị cancel trong sleep → timestamp đã tồn tại nhưng request chưa gửi.
         Rate limiter "nghĩ" đã dùng 1 slot → các coroutine khác phải chờ thêm.
         Tự heal sau 60s nhưng gây rate limit sai trong thời gian đó.
  Sau:  Wrap jitter sleep trong try/except CancelledError.
        Nếu cancel: xóa timestamp vừa thêm (rollback), re-raise.
        now được lưu lại trước khi thêm timestamp để dùng làm key khi remove.
        list.remove() xóa lần xuất hiện đầu tiên — safe vì mỗi acquire() thêm
        đúng 1 timestamp với giá trị time.monotonic() khác nhau.
"""
import asyncio
import random
import time

from google import genai

from config import GEMINI_API_KEY, AI_MAX_RPM, AI_JITTER

ai_client = genai.Client(api_key=GEMINI_API_KEY)


class AIRateLimiter:
    """
    Token bucket — giới hạn AI calls / phút.
    Lock được release TRƯỚC khi sleep để không block các coroutine khác.
    """

    def __init__(self, max_rpm: int = AI_MAX_RPM) -> None:
        self.max_rpm    = max_rpm
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        # ── Bước 1: chờ đến khi có slot trống, rồi claim slot ────────────────
        claimed_at: float | None = None

        while True:
            async with self._lock:
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < 60.0]
                if len(self._timestamps) < self.max_rpm:
                    # Claim slot: lưu timestamp ngay trong lock
                    self._timestamps.append(now)
                    claimed_at = now
                    break
                oldest   = self._timestamps[0]
                wait_sec = 60.0 - (now - oldest) + 0.1
            print(f"  [AI] ⏳ Rate limit: chờ {wait_sec:.1f}s...", flush=True)
            await asyncio.sleep(wait_sec)

        # ── Bước 2: jitter sleep ngoài lock ──────────────────────────────────
        # FIX-RATELEAK: nếu bị cancel tại đây, request chưa được gửi
        # nhưng timestamp đã được claim ở Bước 1. Cần rollback.
        lo, hi = AI_JITTER
        try:
            await asyncio.sleep(random.uniform(lo, hi))
        except asyncio.CancelledError:
            # Rollback: xóa timestamp vừa claim để không "lãng phí" rate limit slot
            if claimed_at is not None:
                async with self._lock:
                    try:
                        self._timestamps.remove(claimed_at)
                    except ValueError:
                        pass  # đã bị expire và xóa bởi acquire() khác — ok
            raise