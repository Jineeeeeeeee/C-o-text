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


_RE_SITE_SUFFIX = re.compile(
    r"\s*[\|–\-—]\s*[A-Za-z0-9][A-Za-z0-9 .]{3,40}$",
    re.UNICODE,
)


def strip_site_suffix(raw: str) -> str:
    """
    Xóa site name suffix khỏi <title> tag.
    Chỉ dùng cho nguồn 'title_tag' và 'og:title' — KHÔNG dùng cho h1/h2/dedicated selector.

    VD: "Chapter 5 | RoyalRoad" → "Chapter 5"
        "Chapter 5 – The Wandering Inn" → "Chapter 5"
        "Chapter 5 - Into the Dark" → "Chapter 5"  ← đây là BUG nên tránh
    """
    return _RE_SITE_SUFFIX.sub("", raw).strip()


# ── Safe filename ─────────────────────────────────────────────────────────────

_RE_UNSAFE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')
_WINDOWS_RESERVED = frozenset({
    "CON","PRN","AUX","NUL",
    "COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
    "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9",
})


def slugify_filename(name: str, max_len: int = 80) -> str:
    safe = _RE_UNSAFE.sub("_", name)
    safe = re.sub(r"_+", "_", safe).strip("_. ")
    if safe.split(".")[0].upper() in _WINDOWS_RESERVED:
        safe = f"_{safe}"
    return safe[:max_len] or "_"


# ── Misc ──────────────────────────────────────────────────────────────────────

def truncate(text: str, n: int, ellipsis: str = "…") -> str:
    return text if len(text) <= n else text[:n - len(ellipsis)] + ellipsis