"""
learning/profile_manager.py — Quản lý SiteProfile per-domain.

Thread-safe qua asyncio.Lock (chia sẻ từ AppState).
Một ProfileManager instance tồn tại suốt phiên và được truyền vào tất cả tasks.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from utils.file_io import save_profiles
from utils.types   import SiteProfile

logger = logging.getLogger(__name__)


class ProfileManager:
    """
    Wrapper thread-safe cho dict[domain → SiteProfile].

    Khởi tạo 1 lần trong AppState với profiles load từ disk.
    Tất cả write operation dùng self._lock.
    Read operations (get, has) không cần lock (Python GIL đủ cho dict read).
    """

    def __init__(self, profiles: dict[str, SiteProfile], lock: asyncio.Lock) -> None:
        self._profiles = profiles
        self._lock     = lock
        self._dirty    = False

    # ── Read (no lock) ────────────────────────────────────────────────────────

    def get(self, domain: str) -> SiteProfile:
        return self._profiles.get(domain, {})  # type: ignore[return-value]

    def has(self, domain: str) -> bool:
        return domain in self._profiles

    def is_profile_fresh(self, domain: str) -> bool:
        """True nếu profile mới hơn PROFILE_MAX_AGE_DAYS."""
        from config import PROFILE_MAX_AGE_DAYS
        p = self._profiles.get(domain)
        if not p or not p.get("last_learned"):
            return False
        try:
            learned = datetime.fromisoformat(p["last_learned"])
            age = (datetime.now(timezone.utc) - learned).days
            return age < PROFILE_MAX_AGE_DAYS
        except Exception:
            return False

    def summary(self, domain: str) -> str:
        p = self._profiles.get(domain)
        if not p:
            return f"{domain}: no profile"
        return (
            f"{domain}: "
            f"conf={p.get('confidence', 0):.2f} | "
            f"content={p.get('content_selector')!r} | "
            f"nav={p.get('nav_type')!r} | "
            f"tables={p.get('formatting_rules', {}).get('tables', False)} | "
            f"math={p.get('formatting_rules', {}).get('math_support', False)}"
        )

    # ── Write (with lock) ─────────────────────────────────────────────────────

    async def save_profile(self, domain: str, profile: SiteProfile) -> None:
        """Lưu profile mới hoàn toàn cho domain."""
        async with self._lock:
            self._profiles[domain] = profile
            self._dirty = True

    async def update_field(self, domain: str, key: str, value) -> None:
        """Cập nhật một field trong profile."""
        async with self._lock:
            p = self._profiles.setdefault(domain, {})  # type: ignore[misc]
            p[key]      = value  # type: ignore[literal-required]
            self._dirty = True

    async def flush(self) -> None:
        """Lưu tất cả profiles xuống disk nếu có thay đổi."""
        if not self._dirty:
            return
        try:
            async with self._lock:
                await save_profiles(self._profiles)
                self._dirty = False
            logger.debug("[ProfileManager] Profiles saved to disk.")
        except Exception as e:
            logger.error("[ProfileManager] Lưu thất bại: %s", e)


