"""
utils/file_io.py — Async-safe file I/O.
Pattern: .tmp + os.replace() để atomic write.
asyncio.to_thread() cho tất cả I/O blocking.
"""
from __future__ import annotations

import asyncio
import json
import os

from config import DATA_DIR, PROFILES_FILE, PROGRESS_DIR
from utils.types import ProgressDict, SiteProfile


# ── Directory setup ───────────────────────────────────────────────────────────

def ensure_dirs() -> None:
    for d in (DATA_DIR, PROGRESS_DIR, "output"):
        os.makedirs(d, exist_ok=True)


# ── Profiles (site_profiles.json) ─────────────────────────────────────────────

def _sync_load_profiles() -> dict[str, SiteProfile]:
    if not os.path.exists(PROFILES_FILE):
        return {}
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _sync_save_profiles(profiles: dict[str, SiteProfile]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = PROFILES_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PROFILES_FILE)


async def load_profiles() -> dict[str, SiteProfile]:
    return await asyncio.to_thread(_sync_load_profiles)


async def save_profiles(profiles: dict[str, SiteProfile]) -> None:
    await asyncio.to_thread(_sync_save_profiles, profiles)


# ── Progress (progress/<hash>.json) ───────────────────────────────────────────

def _default_progress() -> ProgressDict:
    return ProgressDict(
        current_url      = None,
        chapter_count    = 0,
        story_title      = None,
        all_visited_urls = [],
        fingerprints     = [],
        story_id         = None,
        story_id_regex   = None,
        story_id_locked  = False,
        completed        = False,
        completed_at_url = None,
        learning_done    = False,
        start_url        = "",
    )


def _sync_load_progress(path: str) -> ProgressDict:
    if not os.path.exists(path):
        return _default_progress()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data: dict = json.load(f)
        # Backfill key mới nếu progress cũ thiếu
        defaults = _default_progress()
        for k, v in defaults.items():
            data.setdefault(k, v)
        return data  # type: ignore[return-value]
    except Exception:
        return _default_progress()


def _sync_save_progress(path: str, data: ProgressDict) -> None:
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


async def load_progress(path: str) -> ProgressDict:
    return await asyncio.to_thread(_sync_load_progress, path)


async def save_progress(path: str, data: ProgressDict) -> None:
    await asyncio.to_thread(_sync_save_progress, path, data)


# ── Markdown output ───────────────────────────────────────────────────────────

def _sync_write_markdown(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


async def write_markdown(path: str, content: str) -> None:
    await asyncio.to_thread(_sync_write_markdown, path, content)