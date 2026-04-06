"""
learning/naming.py — Xác định story name và chapter naming pattern.

run_naming_phase():
  - Nếu có pre_fetched_titles từ Learning Phase → dùng luôn (0 extra fetch)
  - Nếu không → fetch _NAMING_SAMPLE_SIZE chapters để lấy raw <title> tags
  - Gọi AI để extract story_name, chapter_keyword, has_subtitle
  - Trả về naming dict để merge vào ProgressDict

Fix L3: thống nhất số lượng title samples về _NAMING_SAMPLE_SIZE = 5.
  Trước đây:
    - fetch path:        _NAMING_FETCH_COUNT = 3  → AI nhận 3 titles
    - pre-fetched path:  [:5]                     → AI nhận 5 titles
  Hai code paths cho AI số lượng data khác nhau → naming quality không nhất quán.
  Bây giờ cả hai paths đều dùng _NAMING_SAMPLE_SIZE = 5.

Output dict:
  story_name_clean     : str   — "Monster, No, I'm a Cultivator!"
  chapter_keyword      : str   — "Chapter" / "Episode" / "Ch." / ...
  has_chapter_subtitle : bool  — True nếu chapters có subtitle sau số
  story_prefix_strip   : str   — prefix cần bóc trước chapter keyword trong title
  output_dir_final     : str   — "output/Monster, No, I'm a Cultivator!"
"""
from __future__ import annotations

import asyncio
import logging
import os

from bs4 import BeautifulSoup

from config import OUTPUT_DIR, get_delay
from core.fetch import fetch_page
from core.navigator import find_next_url
from utils.string_helpers import is_junk_page, slugify_filename
from utils.types import SiteProfile
from ai.client import AIRateLimiter
from ai.agents import ai_extract_naming_rules

logger = logging.getLogger(__name__)

# Fix L3: một hằng số duy nhất cho cả hai paths.
# 5 titles đủ để AI phân biệt story name vs chapter keyword vs site suffix
# mà không tốn quá nhiều fetch requests.
_NAMING_SAMPLE_SIZE: int = 5


async def run_naming_phase(
    chapter1_url       : str,
    pool,
    pw_pool,
    ai_limiter         : AIRateLimiter,
    profile            : SiteProfile,
    pre_fetched_titles : list[str] | None = None,
) -> dict | None:
    """
    Xác định naming rules cho story mới. Chạy đúng 1 lần, kết quả lưu vào progress.

    Args:
        chapter1_url:       URL của Chapter 1 (đã được xác định bởi find_start_chapter)
        pre_fetched_titles: Raw <title> tags từ Learning Phase — nếu có thì 0 extra fetch
        profile:            Site profile để navigate (next_selector, nav_type, ...)

    Returns:
        Dict với naming rules, hoặc None nếu AI thất bại (caller dùng fallback dir)
    """
    from utils.string_helpers import domain_tag as _dtag
    tag = _dtag(chapter1_url)

    if pre_fetched_titles:
        # Fix L3: dùng _NAMING_SAMPLE_SIZE thay vì hardcode [:5]
        raw_titles = [t for t in pre_fetched_titles if t][:_NAMING_SAMPLE_SIZE]
        print(
            f"  [{tag}] 🏷  Naming: sử dụng {len(raw_titles)} titles"
            f" từ Learning Phase (max={_NAMING_SAMPLE_SIZE})",
            flush=True,
        )
    else:
        # Fix L3: fetch đủ _NAMING_SAMPLE_SIZE chapters thay vì chỉ 3
        print(
            f"  [{tag}] 🏷  Naming: fetch {_NAMING_SAMPLE_SIZE} chapters"
            f" để lấy titles...",
            flush=True,
        )
        raw_titles = await _fetch_titles(
            chapter1_url, pool, pw_pool, profile,
            n=_NAMING_SAMPLE_SIZE,
        )

    if not raw_titles:
        logger.warning("[Naming] Không lấy được titles — dùng fallback output dir")
        return None

    print(
        f"  [{tag}] 🤖 AI naming: phân tích {len(raw_titles)} titles...",
        flush=True,
    )
    result = await ai_extract_naming_rules(raw_titles, chapter1_url, ai_limiter)

    if not result or not result.get("story_name"):
        logger.warning("[Naming] AI thất bại — dùng fallback output dir")
        return None

    story_name = result["story_name"].strip()
    story_slug = slugify_filename(story_name, max_len=80)
    output_dir = os.path.join(OUTPUT_DIR, story_slug)

    naming = {
        "story_name_clean"    : story_name,
        "chapter_keyword"     : result.get("chapter_keyword", "Chapter").strip(),
        "has_chapter_subtitle": bool(result.get("has_chapter_subtitle", False)),
        "story_prefix_strip"  : (result.get("story_prefix_to_strip") or "").strip(),
        "output_dir_final"    : output_dir,
    }

    print(
        f"  [{tag}] ✅ Story: {story_name!r}\n"
        f"     keyword={naming['chapter_keyword']!r} | "
        f"subtitle={naming['has_chapter_subtitle']} | "
        f"prefix={naming['story_prefix_strip']!r}\n"
        f"     → {output_dir}",
        flush=True,
    )
    return naming


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _fetch_titles(
    chapter1_url : str,
    pool,
    pw_pool,
    profile      : SiteProfile,
    n            : int = _NAMING_SAMPLE_SIZE,   # Fix L3: default = hằng số chung
) -> list[str]:
    """Fetch n chapters liên tiếp, trả về list raw <title> tag content."""
    titles  : list[str] = []
    current : str | None = chapter1_url

    for i in range(n):
        if not current:
            break
        try:
            status, html = await fetch_page(current, pool, pw_pool)
            if is_junk_page(html, status):
                break

            title = _get_title_tag(html)
            if title:
                titles.append(title)

            if i < n - 1:
                soup     = BeautifulSoup(html, "html.parser")
                next_url = find_next_url(soup, current, profile)
                if not next_url or next_url == current:
                    break
                current = next_url
                await asyncio.sleep(get_delay(current))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[Naming] Fetch chapter %d thất bại: %s", i + 1, e)
            break

    return titles


def _get_title_tag(html: str) -> str | None:
    """Lấy nội dung <title> tag từ HTML. Fallback sang h1 nếu không có."""
    soup = BeautifulSoup(html, "html.parser")
    t = soup.find("title")
    if t:
        return t.get_text(strip=True)
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)
    return None


def get_raw_title_from_html(html: str) -> str | None:
    """Public helper — dùng bởi learning/phase.py để extract titles từ chapter HTML."""
    return _get_title_tag(html)