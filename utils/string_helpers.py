"""
utils/string_helpers.py — Pure utility functions, không import từ module nội bộ nào.

Functions:
    domain_tag()            — short display tag cho logging (move từ core/scraper._dtag)
    normalize_title()       — chuẩn hóa chapter title
    strip_site_suffix()     — bóc "| Royal Road", "- FanFiction.net", v.v.
    slugify_filename()      — tạo tên file an toàn từ title
    truncate()              — cắt string với ellipsis
    make_fingerprint()      — MD5 hash cho dedup
    is_junk_page()          — kiểm tra HTML có phải junk/error page không
    is_cloudflare_challenge() — kiểm tra có phải CF challenge không

Fix M1: slugify_filename() — 3 cải thiện cho Windows safety:
    1. NFC → NFKC: normalize compatibility chars (ﬁ→fi, ²→2, v.v.)
    2. Windows reserved names (CON, NUL, COM1...) → thêm suffix "_file"
    3. Post-truncate trailing dot check — truncate có thể tạo ra trailing dot mới
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from urllib.parse import urlparse


# ── domain_tag ─────────────────────────────────────────────────────────────────

def domain_tag(url_or_domain: str) -> str:
    """
    Short display tag cho console logging.

    Examples:
        domain_tag("https://www.royalroad.com/fiction/123") → "royalroad   "
        domain_tag("www.fanfiction.net")                    → "fanfiction  "
        domain_tag("novelfire.net")                         → "novelfire   "
    """
    if url_or_domain.startswith("http"):
        netloc = urlparse(url_or_domain).netloc.lower()
    else:
        netloc = url_or_domain.lower()
    name = netloc.replace("www.", "").split(".")[0]
    return f"{name[:12]:<12}"


# ── normalize_title ────────────────────────────────────────────────────────────

_SITE_SUFFIX = re.compile(
    r"\s*[\|–—\-]\s*(?:royal\s*road|scribblehub|wattpad|fanfiction\.net"
    r"|archiveofourown\.org|ao3|webnovel|novelfire|novelupdates"
    r"|lightnovelreader|novelfull|wuxiaworld|readlightnovel"
    r"|[a-z0-9\-]+\.(?:com|net|org|io))\s*$",
    re.IGNORECASE,
)


def normalize_title(text: str) -> str:
    """
    Chuẩn hóa chapter/story title:
        - Strip whitespace đầu cuối
        - Chuẩn hóa khoảng trắng bên trong
        - Loại bỏ ký tự control (U+0000 - U+001F)

    Examples:
        normalize_title("  Chapter  5  –  The Rise  ") → "Chapter 5 – The Rise"
        normalize_title("Prologue\x00") → "Prologue"
    """
    if not text:
        return ""
    text = re.sub(r"[\x00-\x1f\x7f]", "", text)
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text


def strip_site_suffix(text: str) -> str:
    """
    Bóc site suffix từ title.

    Examples:
        strip_site_suffix("Chapter 5 | Royal Road")   → "Chapter 5"
        strip_site_suffix("Prologue - FanFiction.net") → "Prologue"
        strip_site_suffix("Chapter 5 – The Rise")     → "Chapter 5 – The Rise"
    """
    return _SITE_SUFFIX.sub("", text).strip()


# ── slugify_filename ───────────────────────────────────────────────────────────

_SLUG_REPLACE = {
    "–": "-", "—": "-", "…": "...", "'": "'", "'": "'",
    "\u201c": '"', "\u201d": '"', "«": '"', "»": '"',
    "×": "x", "÷": "-", "©": "", "®": "", "™": "",
    "→": "-", "←": "-", "↑": "", "↓": "",
    "★": "", "☆": "", "♥": "", "♦": "", "♠": "", "♣": "",
    "•": "-", "·": "-", "。": ".", "，": ",",
}

_SLUG_UNSAFE  = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SLUG_SPACES  = re.compile(r"[\s_]+")
_SLUG_DOTS    = re.compile(r"\.{2,}")
_SLUG_EDGES   = re.compile(r"^[\s.\-_]+|[\s.\-_]+$")
_SLUG_MULTI   = re.compile(r"-{2,}")

# Windows reserved filenames (case-insensitive, with or without extension).
# Tạo file với tên này trên Windows sẽ fail hoặc trỏ vào device handle.
_WIN_RESERVED = re.compile(
    r"^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])$",
    re.IGNORECASE,
)


def slugify_filename(text: str, max_len: int = 80) -> str:
    """
    Tạo tên file an toàn từ title — an toàn trên cả Windows và Linux/macOS.

    Fix M1 — 3 thay đổi so với phiên bản cũ:

    1. NFKC thay vì NFC:
       NFC giữ nguyên compatibility characters (ﬁ, ², ™, v.v.).
       NFKC decompose chúng thành dạng ASCII tương đương (fi, 2, TM)
       trước khi các bước xử lý tiếp theo, tránh ký tự "lạ" trong filename.

    2. Windows reserved name guard:
       CON.md, NUL.md, COM1.md là invalid trên Windows — tạo file sẽ fail
       hoặc (tệ hơn) trỏ vào device handle mà không báo lỗi.
       Append "_file" suffix để tránh: "CON" → "CON_file".

    3. Post-truncate trailing dot strip:
       _SLUG_EDGES strip trailing dot trước truncate. Nhưng nếu truncate
       cắt đúng vào vị trí tạo ra trailing dot mới (VD: "abc." sau khi
       cắt "abc.xyz"), Windows silently bỏ dot → tên file thay đổi ngoài
       ý muốn. Strip lần thứ hai sau truncate để đảm bảo.

    Examples:
        slugify_filename("Chapter 5 – The Rise!")  → "Chapter_5_-_The_Rise"
        slugify_filename("Hello: World?")           → "Hello_World"
        slugify_filename("CON")                     → "CON_file"
        slugify_filename("ﬁle name²")               → "file_name2"  (NFKC)
        slugify_filename("A" * 200, max_len=80)     → "A" * 80
    """
    if not text:
        return "untitled"

    # Fix M1-1: NFKC thay vì NFC
    # NFKC decompose compatibility chars: ﬁ→fi, ²→2, ™→TM, v.v.
    # NFC chỉ normalize dạng tổ hợp nhưng giữ nguyên compatibility chars.
    text = unicodedata.normalize("NFKC", text)

    # Replace typographic chars
    for src, dst in _SLUG_REPLACE.items():
        text = text.replace(src, dst)

    # Loại bỏ ký tự không an toàn cho filesystem
    text = _SLUG_UNSAFE.sub("", text)

    # Spaces/tabs → underscore
    text = _SLUG_SPACES.sub("_", text)

    # Multiple dots → single
    text = _SLUG_DOTS.sub(".", text)

    # Strip leading/trailing unsafe chars (lần 1 — trước truncate)
    text = _SLUG_EDGES.sub("", text)

    # Truncate
    if len(text) > max_len:
        text = text[:max_len]
        # Fix M1-3: strip lại SAU truncate vì truncate có thể tạo trailing dot mới
        # VD: "Chapter_5.xyz"[:10] → "Chapter_5." → phải strip dot cuối
        text = _SLUG_EDGES.sub("", text)

    if not text:
        return "untitled"

    # Fix M1-2: Windows reserved name guard
    # Kiểm tra stem (phần trước dấu chấm cuối nếu có)
    stem = text.rsplit(".", 1)[0] if "." in text else text
    if _WIN_RESERVED.match(stem):
        text = text + "_file"

    return text


# ── truncate ───────────────────────────────────────────────────────────────────

def truncate(text: str, max_len: int, ellipsis: str = "…") -> str:
    """
    Cắt string, thêm ellipsis nếu bị cắt.

    Examples:
        truncate("Hello World", 8)  → "Hello W…"
        truncate("Hello", 10)       → "Hello"
    """
    if len(text) <= max_len:
        return text
    return text[:max_len - len(ellipsis)] + ellipsis


# ── make_fingerprint ───────────────────────────────────────────────────────────

def make_fingerprint(content: str) -> str:
    """
    Tạo MD5 fingerprint từ content để dedup chapters.

    Returns:
        16-char hex string (128-bit MD5, đủ cho dedup, không cần security).
    """
    normalized = re.sub(r"\s+", " ", content.strip())
    return hashlib.md5(normalized.encode("utf-8", errors="replace")).hexdigest()[:16]


# ── is_junk_page ───────────────────────────────────────────────────────────────

_JUNK_PATTERNS = [
    re.compile(r"<title>[^<]*404[^<]*</title>",           re.IGNORECASE),
    re.compile(r"<title>[^<]*not found[^<]*</title>",     re.IGNORECASE),
    re.compile(r"<title>[^<]*error[^<]*</title>",         re.IGNORECASE),
    re.compile(r"<title>[^<]*access denied[^<]*</title>", re.IGNORECASE),
    re.compile(r"<title>[^<]*forbidden[^<]*</title>",     re.IGNORECASE),
]

_JUNK_STATUSES = frozenset({400, 401, 403, 404, 410, 429, 500, 502, 503, 504})


def is_junk_page(html: str, status: int = 200) -> bool:
    """
    Kiểm tra response có phải junk/error page không.
    """
    if not html or len(html.strip()) < 200:
        return True
    if status in _JUNK_STATUSES:
        return True
    for pattern in _JUNK_PATTERNS:
        if pattern.search(html[:2000]):
            return True
    return False


# ── is_cloudflare_challenge ────────────────────────────────────────────────────

_CF_PATTERNS = [
    re.compile(r"<title>[^<]*just a moment[^<]*</title>", re.IGNORECASE),
    re.compile(r"<title>[^<]*cloudflare[^<]*</title>",    re.IGNORECASE),
    re.compile(r"cf-browser-verification",                re.IGNORECASE),
    re.compile(r"checking your browser",                  re.IGNORECASE),
    re.compile(r"enable javascript and cookies",          re.IGNORECASE),
    re.compile(r"ray id.*cloudflare",                     re.IGNORECASE),
    re.compile(r'id="challenge-form"',                    re.IGNORECASE),
    re.compile(r"__cf_chl_opt",                           re.IGNORECASE),
]


def is_cloudflare_challenge(html: str) -> bool:
    """
    Kiểm tra response có phải Cloudflare challenge page không.
    """
    if not html or len(html) < 100:
        return False
    sample = html[:5000]
    return any(p.search(sample) for p in _CF_PATTERNS)


# ── Backward compatibility alias ──────────────────────────────────────────────
_dtag = domain_tag