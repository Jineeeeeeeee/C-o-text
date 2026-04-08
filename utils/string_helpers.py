"""
utils/string_helpers.py — Pure utility functions, không import từ module nội bộ nào.

Fix M1: slugify_filename() — NFKC, Windows reserved names, post-truncate dot strip.
Fix P0-4: xóa 429 khỏi _JUNK_STATUSES.
  429 là rate limit TẠM THỜI, không phải lỗi vĩnh viễn. Story không nên
  dừng vĩnh viễn sau một lần bị rate limit.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from urllib.parse import urlparse


# ── domain_tag ─────────────────────────────────────────────────────────────────

def domain_tag(url_or_domain: str) -> str:
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
    if not text:
        return ""
    text = re.sub(r"[\x00-\x1f\x7f]", "", text)
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text


def strip_site_suffix(text: str) -> str:
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

_SLUG_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SLUG_SPACES = re.compile(r"[\s_]+")
_SLUG_DOTS   = re.compile(r"\.{2,}")
_SLUG_EDGES  = re.compile(r"^[\s.\-_]+|[\s.\-_]+$")

_WIN_RESERVED = re.compile(r"^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])$", re.IGNORECASE)


def slugify_filename(text: str, max_len: int = 80) -> str:
    if not text:
        return "untitled"
    text = unicodedata.normalize("NFKC", text)
    for src, dst in _SLUG_REPLACE.items():
        text = text.replace(src, dst)
    text = _SLUG_UNSAFE.sub("", text)
    text = _SLUG_SPACES.sub("_", text)
    text = _SLUG_DOTS.sub(".", text)
    text = _SLUG_EDGES.sub("", text)
    if len(text) > max_len:
        text = text[:max_len]
        text = _SLUG_EDGES.sub("", text)
    if not text:
        return "untitled"
    stem = text.rsplit(".", 1)[0] if "." in text else text
    if _WIN_RESERVED.match(stem):
        text = text + "_file"
    return text


# ── truncate ───────────────────────────────────────────────────────────────────

def truncate(text: str, max_len: int, ellipsis: str = "…") -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - len(ellipsis)] + ellipsis


# ── make_fingerprint ───────────────────────────────────────────────────────────

def make_fingerprint(content: str) -> str:
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

# Fix P0-4: 429 REMOVED — rate limit tạm thời, không phải lỗi vĩnh viễn.
# 429 handling: scraper.py report BLOCKED + consecutive_errors guard.
_JUNK_STATUSES = frozenset({400, 401, 403, 404, 410, 500, 502, 503, 504})


def is_junk_page(html: str, status: int = 200) -> bool:
    """
    Kiểm tra response có phải junk/error page không.
    429 không được coi là junk — caller xử lý qua retry/backoff.
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
    if not html or len(html) < 100:
        return False
    sample = html[:5000]
    return any(p.search(sample) for p in _CF_PATTERNS)


# ── Backward compatibility alias ──────────────────────────────────────────────
_dtag = domain_tag