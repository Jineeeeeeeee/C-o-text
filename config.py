"""
config.py — Hằng số, regex và helpers thuần túy.
Không import từ module nội bộ nào.
"""
import os
import re
import random
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env")
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ── API ───────────────────────────────────────────────────────────────────────
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
    raise SystemExit("[ERR] Không tìm thấy GEMINI_API_KEY trong .env")

GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# ── Giới hạn scraper ──────────────────────────────────────────────────────────
MAX_CHAPTERS             = 5000
MAX_CONSECUTIVE_ERRORS   = 5
MAX_CONSECUTIVE_TIMEOUTS = 3
TIMEOUT_BACKOFF_BASE     = 30   # seconds

# ── Empty streak / retry ──────────────────────────────────────────────────────
MAX_EMPTY_STREAK  = 10   # Số chapters rỗng liên tiếp trước khi thử recover (tăng từ 5→10)
MAX_EMPTY_RETRIES = 1    # Số lần retry sau khi nghi rate-limit
EMPTY_BACKOFF     = 60   # seconds chờ khi nghi rate-limit

# ── Paths ─────────────────────────────────────────────────────────────────────
DATA_DIR      = "data"
OUTPUT_DIR    = "output"
PROGRESS_DIR  = "progress"
PROFILES_FILE = os.path.join(DATA_DIR, "site_profiles.json")
ADS_DB_FILE   = os.path.join(DATA_DIR, "ads_keywords.json")

# ── Learning phase ────────────────────────────────────────────────────────────
LEARNING_CHAPTERS    = 10   # Số chương fetch để học (tăng từ 5 → 10)
LEARNING_MIN_CONTENT = 300  # Chars tối thiểu để content hợp lệ
PROFILE_MAX_AGE_DAYS = 30   # Re-learn nếu profile cũ hơn N ngày

# Số AI calls trong learning phase
LEARNING_AI_CALLS = 10

# Ngưỡng conflict: nếu 2 AI độc lập disagree trên >= N fields → flag uncertain
LEARNING_CONFLICT_THRESHOLD = 3

# ── AI ────────────────────────────────────────────────────────────────────────
AI_MAX_RPM = 10
AI_JITTER  = (0.5, 2.0)

# ── HTTP ──────────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 60

# ── Misc ──────────────────────────────────────────────────────────────────────
INIT_STAGGER = 2.0  # seconds giữa các task khi khởi động

# ── Chrome fingerprint rotation ───────────────────────────────────────────────
CHROME_VERSIONS = ["chrome119", "chrome120", "chrome123", "chrome124", "chrome131"]
CHROME_UA: dict[str, str] = {
    "chrome119": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "chrome120": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "chrome123": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "chrome124": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "chrome131": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

def pick_chrome_version() -> str:
    return random.choice(CHROME_VERSIONS)

def make_headers(version: str) -> dict[str, str]:
    return {
        "User-Agent"               : CHROME_UA.get(version, CHROME_UA["chrome124"]),
        "Accept"                   : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language"          : "en-US,en;q=0.9",
        "Accept-Encoding"          : "gzip, deflate, br",
        "Connection"               : "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

# ── Delay profiles theo domain ────────────────────────────────────────────────
_DELAY_PROFILES: dict[str, tuple[float, float]] = {
    "royalroad.com"       : (6.0, 14.0),
    "www.royalroad.com"   : (6.0, 14.0),
    "scribblehub.com"     : (4.0, 10.0),
    "www.scribblehub.com" : (4.0, 10.0),
    "wattpad.com"         : (3.0,  8.0),
    "www.wattpad.com"     : (3.0,  8.0),
    "fanfiction.net"      : (4.0, 10.0),
    "www.fanfiction.net"  : (4.0, 10.0),
    "archiveofourown.org" : (2.0,  5.0),
    "www.webnovel.com"    : (3.0,  7.0),
}
_DEFAULT_DELAY = (1.0, 3.0)

def get_delay(url: str) -> float:
    domain = urlparse(url).netloc.lower()
    lo, hi = _DELAY_PROFILES.get(domain, _DEFAULT_DELAY)
    return random.uniform(lo, hi)

# ── Fallback selectors (trước khi có profile) ─────────────────────────────────
FALLBACK_CONTENT_SELECTORS: list[str] = [
    "#chapter-c",
    "#chr-content",
    "div.chapter-content",
    ".chapter-content",
    "article",
    "[itemprop='articleBody']",
    "#storytext",
    "div.text-left",
    "div.entry-content",
]

# ── Regex compile sẵn ────────────────────────────────────────────────────────
RE_CHAP_URL = re.compile(
    r"(?:chapter|chuong|chap)[_-]?\d+"        # chapter-5, chap_3, chuong2
    r"|/ch?[/_-]\d+"                           # /c/123, /c_123, /ch/5, /ch-5
    r"|(?:episode|ep|part)[_-]?\d+"            # episode-3, ep_5, part-2
    r"|/s/\d+/\d+",                            # fanfiction.net /s/123/5/
    re.IGNORECASE,
)

RE_NEXT_BTN = re.compile(
    r"\b(next|tiếp|sau|next\s*chapter|chương\s*tiếp|siguiente)\b",
    re.IGNORECASE | re.UNICODE,
)

RE_CHAP_HREF = re.compile(
    r"/(?:chapter|chuong|chap)[_-]?\d+"       # /chapter-5
    r"|/ch?[/_-]\d+"                           # /c/123, /ch/5
    r"|/(?:episode|ep|part)[_-]?\d+"          # /episode-3
    r"|/s/\d+/\d+/",                           # fanfiction.net
    re.IGNORECASE,
)

RE_CHAP_KW = re.compile(
    r"\b(chapter|chap|chương|episode|ep|part)\b[\s.\-:]*\d+",
    re.IGNORECASE | re.UNICODE,
)

RE_CHAP_SLUG = re.compile(
    r"(.*?(?:chapter|chuong|chap|/c|/ep|episode|part|phan|tap)[s_-]?)(\d+)(/?(?:[?#].*)?)$",
    re.IGNORECASE,
)

RE_FANFIC = re.compile(r"(/s/\d+/)(\d+)(/.+)?$")