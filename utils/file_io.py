# utils/file_io.py
"""
utils/file_io.py — Toàn bộ thao tác I/O file theo mô hình async-safe.

Tất cả hàm ghi dùng pattern .tmp + os.replace() → atomic write,
tránh corrupt file khi bị kill giữa chừng.

asyncio.to_thread() đẩy I/O đồng bộ xuống thread-pool, giải phóng event loop.

Quy ước:
  _sync_*  → hàm đồng bộ, chỉ gọi qua asyncio.to_thread()
  async    → wrapper công khai
"""
from __future__ import annotations

import asyncio
import json
import os

from config import PROFILES_FILE
from utils.types import ProgressDict, SiteProfileDict


# ── site_profiles.json ────────────────────────────────────────────────────────

def _sync_load_profiles() -> dict[str, SiteProfileDict]:
    if os.path.exists(PROFILES_FILE):
        try:
            with open(PROFILES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _sync_save_profiles(profiles: dict[str, SiteProfileDict]) -> None:
    tmp = PROFILES_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PROFILES_FILE)

async def load_profiles() -> dict[str, SiteProfileDict]:
    return await asyncio.to_thread(_sync_load_profiles)

async def save_profiles(profiles: dict[str, SiteProfileDict]) -> None:
    await asyncio.to_thread(_sync_save_profiles, profiles)


# ── progress file ─────────────────────────────────────────────────────────────

def make_default_progress() -> ProgressDict:
    """Tạo ProgressDict rỗng với toàn bộ key được định nghĩa."""
    return ProgressDict(
        current_url       = None,
        chapter_count     = 0,
        story_title       = None,
        all_visited_urls  = [],
        fingerprints      = [],
        collected_urls    = [],
        story_id          = None,
        story_id_regex    = None,
        story_id_locked   = False,
        story_id_attempts = 0,
        completed         = False,
        completed_at_url  = None,
        last_scraped_url  = None,
        last_title        = None,
    )

def _sync_load_progress(path: str) -> ProgressDict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data: dict = json.load(f)
            # Backfill bất kỳ key mới nào bị thiếu trong progress cũ
            defaults = make_default_progress()
            for k, v in defaults.items():
                data.setdefault(k, v)
            return data  # type: ignore[return-value]
        except Exception:
            pass
    return make_default_progress()

def _sync_save_progress(path: str, data: ProgressDict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    os.replace(tmp, path)

async def load_progress(path: str) -> ProgressDict:
    return await asyncio.to_thread(_sync_load_progress, path)

async def save_progress(path: str, data: ProgressDict) -> None:
    await asyncio.to_thread(_sync_save_progress, path, data)


# ── file .md ──────────────────────────────────────────────────────────────────

def _sync_write_markdown(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

async def write_markdown(path: str, content: str) -> None:
    await asyncio.to_thread(_sync_write_markdown, path, content)
