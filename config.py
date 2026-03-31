"""
config.py — Hằng số cấu hình, regex compile sẵn và helper thuần túy.

Quy tắc:
  • Không import module nội bộ nào khác trong project.
  • Chỉ dùng stdlib + dotenv.
  • Mọi module khác import từ đây; không có chiều ngược lại.
  • Regex compile ở module-level — 1 lần khi import, tái dùng suốt phiên.
"""
import os
import re
import random
from urllib.parse import urlparse

from dotenv import load_dotenv

# Tìm .env từ thư mục project trở lên, tự động
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / ".env")          # thử trong Cào text\
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")   # thử ở Small Project\

GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
    raise SystemExit("[ERR] Không tìm thấy GEMINI_API_KEY trong file .env")

GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# ── Giới hạn scraper ──────────────────────────────────────────────────────────
MAX_CHAPTERS           = 1000
MAX_CONSECUTIVE_ERRORS = 5

# ── Chrome version rotation ───────────────────────────────────────────────────
CHROME_VERSIONS: list[str] = [
    "chrome110", "chrome116", "chrome119", "chrome120",
    "chrome123", "chrome124", "chrome131",
]
CHROME_UA: dict[str, str] = {
    "chrome110": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "chrome116": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "chrome119": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "chrome120": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "chrome123": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "chrome124": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "chrome131": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

def pick_chrome_version() -> str:
    return random.choice(CHROME_VERSIONS)

def make_headers(chrome_version: str) -> dict:
    return {
        "User-Agent"               : CHROME_UA.get(chrome_version, CHROME_UA["chrome120"]),
        "Accept"                   : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language"          : "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding"          : "gzip, deflate, br",
        "Connection"               : "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

# ── Content selectors ─────────────────────────────────────────────────────────
CONTENT_SELECTORS: list[str] = [
    "#chapter-c",
    "#chr-content",
    "div.chapter-content",
    "article",
    "[itemprop='articleBody']",
    "[class*='article-body']",
    "[class*='content-detail']",
    ".chapter-content",
    "#storytext",
]

# ── HTTP timeout ──────────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 60

# ── Delay profile theo domain ─────────────────────────────────────────────────
DELAY_PROFILES: dict[str, tuple[float, float]] = {
    "royalroad.com"       : (8.0,  15.0),
    "www.royalroad.com"   : (8.0,  15.0),
    "scribblehub.com"     : (6.0,  12.0),
    "www.scribblehub.com" : (6.0,  12.0),
    "wattpad.com"         : (4.0,  10.0),
    "www.wattpad.com"     : (4.0,  10.0),
    "fanfiction.net"      : (1.0,   5.0),
    "www.fanfiction.net"  : (3.0,   7.0),
    "archiveofourown.org" : (3.0,   6.0),
}
DEFAULT_CHAPTER_DELAY: tuple[float, float] = (1.0, 3.0)

def get_chapter_delay(url: str) -> tuple[float, float]:
    return DELAY_PROFILES.get(urlparse(url).netloc.lower(), DEFAULT_CHAPTER_DELAY)

def get_delay_seconds(url: str) -> float:
    lo, hi = get_chapter_delay(url)
    return random.uniform(lo, hi)

# ── Timeout / retry ───────────────────────────────────────────────────────────
MAX_CONSECUTIVE_TIMEOUTS = 3
TIMEOUT_BACKOFF_BASE     = 30

# ── Story ID Guard ────────────────────────────────────────────────────────────
STORY_ID_LEARN_AFTER  = 12
STORY_ID_MAX_ATTEMPTS = 3

# ── Misc ──────────────────────────────────────────────────────────────────────
PROFILES_FILE  = "site_profiles.json"
INIT_STAGGER   = 1.5
AI_MAX_RPM     = 3
AI_JITTER      = (2.0, 5.0)

# ── Regex compile sẵn ────────────────────────────────────────────────────────

RE_CHAP_URL = re.compile(
    r"(chapter|chuong|chap|/c|/ch|episode|ep|part|phan|tap)[_-]?\d+$"
    r"|/s/\d+/\d+",      # fanfiction.net: /s/{story_id}/{chapter_num}[/slug]
    re.IGNORECASE,
)
RE_NEXT_BTN = re.compile(
    r"\b(next|tiếp|sau|next\s*chapter|chương\s*tiếp|chương\s*sau|siguiente)\b",
    re.IGNORECASE | re.UNICODE,
)
RE_CHAP_HREF = re.compile(
    r"/(chapter|chuong|chap|c|ep|episode|part)[_-]?\d+"
    r"|/s/\d+/\d+/",     # fanfiction.net chapter href
    re.IGNORECASE,
)
RE_CHAP_KW_URL = re.compile(
    r"\b(chapter|chap|chương|chuong|episode|ep|part)\b[\s.\-:]*\d+",
    re.IGNORECASE | re.UNICODE,
)
RE_CHAP_HINT = re.compile(
    r"(chapter|chuong|chap|/c/|/ch/|episode|ep|phần|tập)\d*"
    r"|/s/\d+/\d+/",     # fanfiction.net: /s/{story_id}/{chapter_num}/
    re.IGNORECASE,
)
RE_NEXT_PREV = re.compile(
    r"\b(next|prev|previous|tiếp|trước|sau|siguiente|anterior)\b",
    re.IGNORECASE | re.UNICODE,
)
RE_CHAP_SLUG = re.compile(
    r"(.*?(?:chapter|chuong|chap|/c|/ep|episode|part|phan|tap)[s_-]?)(\d+)(/?(?:[?#].*)?)$",
    re.IGNORECASE,
)