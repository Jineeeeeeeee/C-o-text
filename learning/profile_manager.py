"""
learning/profile_manager.py — Quản lý SiteProfile per-domain.

Fix #4: add_ads_to_profile() — tính `added` TRƯỚC khi merge vào profile,
  tránh bug cũ luôn trả về 0 do tính sau khi p["ads_keywords_learned"] đã updated.
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
    """

    def __init__(self, profiles: dict[str, SiteProfile], lock: asyncio.Lock) -> None:
        self._profiles = profiles
        self._lock     = lock
        self._dirty    = False

    # ── Read (no lock needed) ─────────────────────────────────────────────────

    def get(self, domain: str) -> SiteProfile:
        return self._profiles.get(domain, {})  # type: ignore[return-value]

    def has(self, domain: str) -> bool:
        return domain in self._profiles

    def is_profile_fresh(self, domain: str) -> bool:
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

    # ── Write (immediate disk flush) ──────────────────────────────────────────

    async def save_profile(self, domain: str, profile: SiteProfile) -> None:
        """
        Lưu profile mới và persist NGAY XUỐNG DISK.
        Không dùng lazy dirty flag — đảm bảo an toàn khi Ctrl+C.
        """
        async with self._lock:
            self._profiles[domain] = profile
            self._dirty = True
            await save_profiles(self._profiles)
        logger.debug("[ProfileManager] Profile saved: %s", domain)

    async def add_ads_to_profile(self, domain: str, keywords: list[str]) -> None:
        """
        Thêm confirmed ads keywords vào profile.ads_keywords_learned + ghi disk ngay.
        Gọi sau khi AI verify xác nhận một keyword là ads thật.

        Fix #4: `added` được tính TRƯỚC khi cập nhật profile (tránh luôn = 0).
        """
        if not keywords:
            return

        async with self._lock:
            p        = self._profiles.setdefault(domain, {})  # type: ignore[misc]
            existing = set(p.get("ads_keywords_learned") or [])
            new_kws  = {kw.lower().strip() for kw in keywords if kw.strip()}

            # Fix #4: tính added TRƯỚC khi merge
            added   = len(new_kws - existing)
            updated = sorted(existing | new_kws)

            p["ads_keywords_learned"] = updated  # type: ignore[typeddict-unknown-key]
            self._dirty = True
            await save_profiles(self._profiles)

        if added > 0:
            logger.debug("[ProfileManager] +%d ads keywords cho %s", added, domain)

    async def update_field(self, domain: str, key: str, value) -> None:
        """Cập nhật một field trong profile (lazy — chờ flush)."""
        async with self._lock:
            p = self._profiles.setdefault(domain, {})  # type: ignore[misc]
            p[key]      = value  # type: ignore[literal-required]
            self._dirty = True

    async def flush(self) -> None:
        """Ghi profiles xuống disk nếu có thay đổi chưa lưu (safety net)."""
        if not self._dirty:
            return
        try:
            async with self._lock:
                await save_profiles(self._profiles)
                self._dirty = False
            logger.debug("[ProfileManager] Profiles flushed to disk.")
        except Exception as e:
            logger.error("[ProfileManager] Flush thất bại: %s", e)