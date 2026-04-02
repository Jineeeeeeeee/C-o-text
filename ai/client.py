"""ai/client.py — Gemini client và AIRateLimiter (token bucket)."""
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
        while True:
            async with self._lock:
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < 60.0]
                if len(self._timestamps) < self.max_rpm:
                    self._timestamps.append(now)
                    break
                oldest   = self._timestamps[0]
                wait_sec = 60.0 - (now - oldest) + 0.1
            print(f"  [AI] ⏳ Rate limit: chờ {wait_sec:.1f}s...", flush=True)
            await asyncio.sleep(wait_sec)

        lo, hi = AI_JITTER
        await asyncio.sleep(random.uniform(lo, hi))