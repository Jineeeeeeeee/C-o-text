"""utils/string_helpers.py — Hàm tiện ích chuỗi, không side-effect."""
import hashlib
import re
import unicodedata

from bs4 import BeautifulSoup

# ── Cloudflare detection ──────────────────────────────────────────────────────

CF_CHALLENGE_TITLES = frozenset({
    "just a moment...", "just a moment",
    "checking your browser before accessing",
    "please wait...", "please wait",
    "attention required!", "one more step",
    "security check", "ddos-guard",
    "enable javascript and cookies to continue",
})


def is_cloudflare_challenge(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    t = soup.find("title")
    return t is not None and t.get_text(strip=True).lower() in CF_CHALLENGE_TITLES


# ── Junk page detection ───────────────────────────────────────────────────────

_ERROR_STATUSES = frozenset({400, 401, 403, 404, 410, 429, 500, 502, 503})
_MIN_BODY_CHARS = 150
_JUNK_TITLE_RE  = re.compile(
    r"\b(404|403|page\s*not\s*found|not\s*found|access\s*denied"
    r"|chapter\s*not\s*found|story\s*not\s*found|story\s*removed"
    r"|chapter\s*unavailable)\b",
    re.IGNORECASE,
)


def is_junk_page(html: str, status: int = 200) -> bool:
    if status in _ERROR_STATUSES:
        return True
    soup = BeautifulSoup(html, "html.parser")
    t = soup.find("title")
    if t and _JUNK_TITLE_RE.search(t.get_text(strip=True)):
        return True
    body = soup.find("body")
    if body and len(body.get_text(separator=" ", strip=True)) < _MIN_BODY_CHARS:
        return True
    return False


# ── Fingerprint ───────────────────────────────────────────────────────────────

def make_fingerprint(text: str) -> str:
    """MD5 của nội dung đã normalize — phát hiện chương lặp."""
    normalized = " ".join(text.lower().split())
    return hashlib.md5(normalized.encode("utf-8", errors="replace")).hexdigest()


# ── Title normalization ───────────────────────────────────────────────────────

_RE_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def normalize_title(raw: str) -> str:
    """Chuẩn hóa title: xóa control chars, normalize unicode, gộp spaces."""
    t = raw.strip()
    t = _RE_CTRL.sub("", t)
    t = unicodedata.normalize("NFC", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = t.strip('"\'')
    return t or "Unknown Title"


# ── strip_site_suffix ─────────────────────────────────────────────────────────
#
# FIX v2: Xử lý đúng hơn cho nhiều loại separator phổ biến:
#
#   OLD: chỉ match [\|–\-—] → "Chapter 9 – Core Strength - Rock falls..."
#        → strip_site_suffix chỉ strip phần sau – (dấu em-dash)
#        → còn lại "Chapter 9 – Core Strength - Rock falls, everyone dies"
#        → candidate quá dài, vote lệch
#
#   NEW: strip tất cả các "suffix block" phân tách bởi |, –, —, hoặc dấu -
#        nhưng CHỈ strip khi phần bị strip trông giống site name / story name,
#        không phải subtitle chương.
#
# Strategy:
#   1. Split theo separator mạnh (|, –, —) trước
#   2. Nếu có ≥2 parts → lấy part ĐẦU TIÊN (thường là chapter title)
#   3. Nếu chỉ có 1 part (chỉ dùng " - ") → strip từ " - " CUỐI CÙNG
#      về sau (heuristic: site suffix thường ở cuối)
#
# VD đúng:
#   "Chapter 9 – Core Strength - Rock falls | Royal Road"
#     → split by | → ["Chapter 9 – Core Strength - Rock falls", "Royal Road"]
#     → lấy part[0] → "Chapter 9 – Core Strength - Rock falls"
#     → vẫn có " - " → strip từ " - " cuối → "Chapter 9 – Core Strength" ✅
#
#   "Chapter 1 – A [Rolling Stone] | The Wandering Inn | Royal Road"
#     → split by | → ["Chapter 1 – A [Rolling Stone]", "The Wandering Inn", "Royal Road"]
#     → lấy part[0] → "Chapter 1 – A [Rolling Stone]" ✅
#
#   "My Novel Chapter 1, a crossover fanfic | FanFiction"
#     → split by | → ["My Novel Chapter 1, a crossover fanfic", "FanFiction"]
#     → lấy part[0] → "My Novel Chapter 1, a crossover fanfic" ✅

# Separator mạnh: |, –, —
_RE_STRONG_SEP = re.compile(r"\s*[|–—]\s*")

# Dấu " - " (hyphen với spaces) — separator yếu hơn
_RE_WEAK_SEP = re.compile(r"\s+-\s+")

# Site name pattern: ngắn (≤40 chars), không có dấu phẩy, không có số chương
_RE_LOOKS_LIKE_SITE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9\s.]{2,39}$"
)


def strip_site_suffix(raw: str) -> str:
    """
    Xóa site name / story name suffix khỏi <title> tag.
    Chỉ dùng cho nguồn 'title_tag' và 'og:title'.

    VD:
      "Chapter 9 – Core Strength - Rock falls | Royal Road"
        → "Chapter 9 – Core Strength"
      "Chapter 1 – Into the Dark | The Wandering Inn | Royal Road"
        → "Chapter 1 – Into the Dark"
      "My Novel Chapter 1, a crossover fanfic | FanFiction"
        → "My Novel Chapter 1, a crossover fanfic"
      "Chapter 9 – Core Strength - Rock falls, everyone dies"  (og:title, no |)
        → "Chapter 9 – Core Strength"
    """
    text = raw.strip()

    # Step 1: Split bởi separator mạnh (|, –, —)
    parts = [p.strip() for p in _RE_STRONG_SEP.split(text) if p.strip()]

    if len(parts) >= 2:
        # Lấy part đầu tiên — thường là chapter title
        text = parts[0].strip()
    # Nếu len == 1: không có separator mạnh → tiếp tục với text gốc

    # Step 2: Kiểm tra có dấu " - " không → strip từ " - " cuối về sau
    # Chỉ strip nếu phần bị strip trông giống site/story name (ngắn, đơn giản)
    weak_parts = _RE_WEAK_SEP.split(text)
    if len(weak_parts) >= 2:
        suffix = weak_parts[-1].strip()
        # Chỉ strip nếu suffix trông như site name / story name
        # Không strip nếu suffix có dấu phẩy (thường là nội dung truyện)
        if "," not in suffix and _RE_LOOKS_LIKE_SITE.match(suffix):
            text = " - ".join(weak_parts[:-1]).strip()

    return text.strip()


# ── Safe filename ─────────────────────────────────────────────────────────────

_RE_UNSAFE = re.compile(r'[\\/:*?"<>|\x00-\x1f\[\]()!\'`~@#$%^&+={}]')
_RE_MULTI_SEP = re.compile(r'[-_\s]{2,}')
_RE_EDGE_SEP  = re.compile(r'^[-_\s]+|[-_\s]+$')

_WINDOWS_RESERVED = frozenset({
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
})


def slugify_filename(name: str, max_len: int = 80) -> str:
    """
    Chuyển tên chương thành filename an toàn trên Windows/Linux/macOS.
    """
    safe = unicodedata.normalize("NFC", name)
    safe = _RE_UNSAFE.sub(" ", safe)
    safe = _RE_MULTI_SEP.sub(" ", safe)
    safe = safe.strip(" -_")
    safe = safe.replace(" ", "_")
    safe = re.sub(r"_+", "_", safe).strip("_")
    if safe.split(".")[0].upper() in _WINDOWS_RESERVED:
        safe = f"_{safe}"
    return safe[:max_len] or "_"


# ── Misc ──────────────────────────────────────────────────────────────────────

def truncate(text: str, n: int, ellipsis: str = "…") -> str:
    return text if len(text) <= n else text[:n - len(ellipsis)] + ellipsis