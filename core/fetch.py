"""
core/fetch.py — fetch_page với Cloudflare fallback tự động.

Flow:
  1. Domain đã biết cần PW → dùng thẳng Playwright
  2. curl_cffi (nhanh, ít overhead)
  3. Gặp CF challenge → đánh dấu domain, retry Playwright
  4. Vẫn bị CF → raise RuntimeError
"""
from __future__ import annotations

from urllib.parse import urlparse

from utils.string_helpers import is_cloudflare_challenge
from core.session_pool import DomainSessionPool, PlaywrightPool


async def fetch_page(
    url: str,
    pool: DomainSessionPool,
    pw_pool: PlaywrightPool,
) -> tuple[int, str]:
    domain = urlparse(url).netloc.lower()

    # Shortcut nếu domain đã được đánh dấu CF
    if pool.is_cf_domain(domain):
        return await pw_pool.fetch(url)

    status, html = await pool.fetch(url)
    if not is_cloudflare_challenge(html):
        return status, html

    print(f"  [CF] {domain} → Playwright mode", flush=True)
    pool.mark_cf_domain(domain)
    status, html = await pw_pool.fetch(url)

    if is_cloudflare_challenge(html):
        raise RuntimeError(f"CF challenge không được giải quyết: {url}")

    return status, html