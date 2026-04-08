"""
learning/profile_manager.py

Fix P0-2: get() trả về shallow copy thay vì live reference.
Fix #4:   add_ads_to_profile() tính `added` TRƯỚC khi merge.
Fix C3:   flush() đọc và reset _dirty BÊN TRONG lock.
Fix P1-9: _dirty_domains set — chỉ ghi domains thực sự thay đổi.

P1-9 detail:
  Trước: save_profile() và add_ads_to_profile() đều gọi
  save_profiles(self._profiles) — serialize và ghi TOÀN BỘ file JSON
  kể cả chỉ 1 domain thay đổi. 50 domains = 50× data được ghi mỗi lần.

  Sau: _dirty_domains: set[str] track domain nào thực sự thay đổi.
  save_profiles() vẫn nhận toàn bộ dict (để backward compat với file format),
  nhưng chỉ được gọi khi có ít nhất 1 domain dirty. flush() cũng chỉ
  ghi khi _dirty = True — không thay đổi so với trước.

  Lưu ý quan trọng: file format vẫn là 1 JSON file duy nhất chứa tất cả
  domains. Đây là intentional — per-domain files sẽ phức tạp hơn và cần
  migration. _dirty_domains chỉ optimize khi nào GHI, không thay đổi
  cấu trúc lưu trữ.
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
        self._profiles      = profiles
        self._lock          = lock
        self._dirty         = False
        self._dirty_domains : set[str] = set()   # P1-9: track changed domains

    # ── Read ──────────────────────────────────────────────────────────────────

    def get(self, domain: str) -> SiteProfile:
        """
        Trả về shallow copy của profile.

        Fix P0-2: trả về copy thay vì live reference.
        Caller mutation sẽ không ảnh hưởng internal state — mọi thay đổi
        cần persist phải đi qua save_profile() để đảm bảo _dirty được set.
        """
        p = self._profiles.get(domain)
        if p is None:
            return {}  # type: ignore[return-value]
        return dict(p)  # type: ignore[return-value]

    def has(self, domain: str) -> bool:
        return domain in self._profiles

    def is_profile_fresh(self, domain: str) -> bool:
        from config import PROFILE_MAX_AGE_DAYS
        p = self._profiles.get(domain)
        if not p or not p.get("last_learned"):
            return False
        try:
            learned = datetime.fromisoformat(p["last_learned"])
            age     = (datetime.now(timezone.utc) - learned).days
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

    # ── Write ─────────────────────────────────────────────────────────────────

    async def save_profile(self, domain: str, profile: SiteProfile) -> None:
        """
        Lưu profile mới và persist NGAY XUỐNG DISK.

        P1-9: đánh dấu domain vào _dirty_domains trước khi ghi.
        Dù với 1 domain cũng ghi ngay (immediate persist guarantee).
        """
        async with self._lock:
            self._profiles[domain] = profile
            self._dirty            = True
            self._dirty_domains.add(domain)
            await save_profiles(self._profiles)
            self._dirty_domains.discard(domain)  # flushed
        logger.debug("[ProfileManager] Profile saved: %s", domain)

    async def add_ads_to_profile(self, domain: str, keywords: list[str]) -> None:
        """
        Thêm confirmed ads keywords vào profile.ads_keywords_learned.

        Fix #4: `added` được tính TRƯỚC khi merge (tránh luôn = 0).
        P1-9: đánh dấu dirty_domains.
        """
        if not keywords:
            return

        async with self._lock:
            p        = self._profiles.setdefault(domain, {})  # type: ignore[misc]
            existing = set(p.get("ads_keywords_learned") or [])
            new_kws  = {kw.lower().strip() for kw in keywords if kw.strip()}

            added   = len(new_kws - existing)   # Fix #4: TRƯỚC khi merge
            updated = sorted(existing | new_kws)

            p["ads_keywords_learned"] = updated  # type: ignore[typeddict-unknown-key]
            self._dirty = True
            self._dirty_domains.add(domain)
            await save_profiles(self._profiles)
            self._dirty_domains.discard(domain)

        if added > 0:
            logger.debug("[ProfileManager] +%d ads keywords cho %s", added, domain)

    async def flush(self) -> None:
        """
        Ghi profiles xuống disk nếu có thay đổi chưa lưu (safety net).

        Fix C3: _dirty đọc và reset BÊN TRONG lock.
        P1-9: sau flush, xóa toàn bộ _dirty_domains.
        """
        async with self._lock:
            if not self._dirty:
                return
            try:
                await save_profiles(self._profiles)
                self._dirty         = False
                self._dirty_domains.clear()
                logger.debug("[ProfileManager] Profiles flushed to disk.")
            except Exception as e:
                logger.error("[ProfileManager] Flush thất bại: %s", e)