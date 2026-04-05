# learning/phase.py
"""
learning/phase.py — 10-Chapter Deep Learning Phase.

Flow:
  1. Fetch 10 chapters (Playwright cho Ch.1, curl_cffi cho Ch.2-10)
  2. 10 AI calls theo 5 phases:
     Phase 1 — Structure Discovery  : AI#1, AI#2 (độc lập), AI#3 (stability)
     Phase 2 — Conflict Resolution  : AI#4 (remove audit), AI#5 (title deepdive)
     Phase 3 — Content Intelligence : AI#6 (special content), AI#7 (ads scan)
     Phase 4 — Stress Test          : AI#8 (nav stress), AI#9 (simulation)
     Phase 5 — Synthesis            : AI#10 (master synthesis)
  3. Save profile → tái dùng 10 chapters đã fetch cho scraping

Sau khi learning xong, 10 chapters được tái dùng:
  - Không fetch lại từ đầu
  - Scrape bắt đầu từ Ch.1 (dùng HTML đã có trong bộ nhớ)
  - Progress lưu URL của Ch.1 để resume an toàn
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from config import (
    LEARNING_CHAPTERS, LEARNING_CONFLICT_THRESHOLD,
    get_delay, RE_CHAP_URL,
)
from utils.types import SiteProfile
from utils.string_helpers import is_junk_page
from core.fetch        import fetch_page
from core.session_pool import DomainSessionPool, PlaywrightPool
from core.navigator    import find_next_url
from learning.profile_manager import ProfileManager
from ai.client  import AIRateLimiter
from ai.agents  import (
    ai_dom_structure,
    ai_independent_check,
    ai_stability_check,
    ai_remove_audit,
    ai_title_deepdive,
    ai_special_content,
    ai_ads_deepscan,
    ai_nav_stress,
    ai_full_simulation,
    ai_master_synthesis,
    ai_classify_and_find,
    ai_find_first_chapter,
    resolve_phase1_conflicts,
)

logger = logging.getLogger(__name__)


# ── Prose & content helpers ──────────────────────────────────────────────────────────

def _html_has_prose(html: str, min_paragraphs: int = 2, min_chars: int = 100) -> bool:
    """Kiểm tra nhanh HTML có chứa prose text hay không (không cần AI)."""
    soup = BeautifulSoup(html, "html.parser")
    for unwanted in soup.find_all(["script", "style", "noscript"]):
        unwanted.decompose()
    prose_count = sum(
        1 for p in soup.find_all("p")
        if len(p.get_text(strip=True)) >= 50
    )
    if prose_count >= min_paragraphs:
        return True
    # Fallback: kiểm tra tổng body text
    body = soup.find("body")
    if body and len(body.get_text(strip=True)) >= min_chars * 3:
        return True
    return False


def _validate_content_richness(
    chapters: list[tuple[str, str]],
) -> tuple[bool, str]:
    """
    Kiểm tra HTML fetch được thực sự chứa text content.
    Dung heuristic nhanh (không cần AI):
      - Đếm chapters có prose (>= 3 đoạn văn ≥ 50 chars)
      - Nếu ≥80% chapters có prose → PASS
      - Nếu <50% → FAIL (đánh dấu JS_RENDERED)
    Returns: (is_valid, reason)
    """
    if not chapters:
        return False, "NO_CHAPTERS"
    rich_count = 0
    for _, html in chapters:
        if _html_has_prose(html, min_paragraphs=3, min_chars=50):
            rich_count += 1
    ratio = rich_count / len(chapters)
    if ratio >= 0.8:
        return True, f"{rich_count}/{len(chapters)} chapters có prose"
    if ratio < 0.5:
        return False, f"JS_RENDERED suspected: chỉ {rich_count}/{len(chapters)} chapters có prose"
    return True, f"{rich_count}/{len(chapters)} chapters (marginal)"


async def _retry_with_playwright(
    chapters  : list[tuple[str, str]],
    pw_pool   : PlaywrightPool,
    domain    : str,
    pool      : DomainSessionPool,
) -> list[tuple[str, str]]:
    """
    Re-fetch các chapters bị empty bằng Playwright.
    Giữ chapters đã có nội dung, chỉ fetch lại chapters rỗng.
    """
    from core.scraper import _dtag
    tag = _dtag(domain)
    result: list[tuple[str, str]] = []
    for url, html in chapters:
        if _html_has_prose(html, min_paragraphs=3):
            result.append((url, html))
        else:
            print(f"  [{tag}] 🔄 Re-fetch (PW): {url[:60]}", flush=True)
            try:
                status, pw_html = await pw_pool.fetch(url)
                if not is_junk_page(pw_html, status):
                    result.append((url, pw_html))
                else:
                    result.append((url, html))  # giữ original
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[%s] Playwright re-fetch thất bại: %s", tag, e)
                result.append((url, html))  # giữ original
        await asyncio.sleep(2.0)
    # Đánh dấu domain cần Playwright
    pool.mark_cf_domain(domain)
    return result


def _find_best_title_fallback(soup: BeautifulSoup) -> str | None:
    """Tìm title selector tốt nhất khi selector hiện tại không hợp lệ."""
    for sel in ("h1", ".chapter-title", ".title", "[class*='chapter-title']",
                "[class*='chapter_title']", "[class*='chaptitle']", "h2"):
        try:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(strip=True)
                if 3 <= len(text) <= 200 and "\n" not in text[:50]:
                    return sel
        except Exception:
            continue
    return None


def _verify_profile_selectors(
    profile  : dict,
    chapters : list[tuple[str, str]],
) -> tuple[dict, list[str]]:
    """
    Chạy selectors thật trên HTML đã fetch và verify kết quả.
    Checks:
      1. content_selector → có trả về ≥20 0chars text?
      2. title_selector → có trả về chuỗi hợp lý (3-200 chars, không dropdown)?
      3. next_selector → có trả về <a> tag với href?
    Returns: (fixed_profile, warnings)
    """
    warnings: list[str] = []
    sample = chapters[:3]

    content_sel = profile.get("content_selector")
    title_sel   = profile.get("title_selector")
    next_sel    = profile.get("next_selector")

    uncertain = list(profile.get("uncertain_fields") or [])

    for url, html in sample:
        soup = BeautifulSoup(html, "html.parser")

        # 1. Content selector verification
        if content_sel:
            try:
                el = soup.select_one(content_sel)
                if el:
                    text = el.get_text(separator=" ", strip=True)
                    if len(text) < 200:
                        warnings.append(
                            f"content_selector {content_sel!r} trả về chỉ {len(text)} chars "
                            f"- có thể là JS-rendered hoặc selector sai"
                        )
                        if "content_selector" not in uncertain:
                            uncertain.append("content_selector")
                else:
                    warnings.append(f"content_selector {content_sel!r} không tìm thấy element")
                    if "content_selector" not in uncertain:
                        uncertain.append("content_selector")
            except Exception as e:
                warnings.append(f"content_selector lỗi: {e}")

        # 2. Title selector sanity check
        if title_sel:
            try:
                el = soup.select_one(title_sel)
                if el:
                    text = el.get_text(strip=True)
                    # Kiểm tra dropdown/list
                    if el.name in ("select", "option", "ul", "ol"):
                        warnings.append(
                            f"title_selector {title_sel!r} là {el.name} tag (dropdown!) "
                            f"— thay bằng fallback"
                        )
                        fb = _find_best_title_fallback(soup)
                        if fb:
                            profile["title_selector"] = fb
                            if "title_selector" not in uncertain:
                                uncertain.append("title_selector")
                        break
                    # Kiểm tra quá dài (có thể là list chapters)
                    if len(text) > 300:
                        warnings.append(
                            f"title_selector {title_sel!r} trả về {len(text)} chars "
                            f"— có thể là dropdown/list"
                        )
                        fb = _find_best_title_fallback(soup)
                        if fb:
                            profile["title_selector"] = fb
                            if "title_selector" not in uncertain:
                                uncertain.append("title_selector")
                        break
                    # Kiểm tra multiline (có thể là list)
                    if text.count("\n") > 5:
                        warnings.append(
                            f"title_selector {title_sel!r} có {text.count(chr(10))} dòng "
                            f"— multiline suspect"
                        )
                        fb = _find_best_title_fallback(soup)
                        if fb:
                            profile["title_selector"] = fb
                        break
            except Exception as e:
                warnings.append(f"title_selector lỗi: {e}")

        # 3. Next selector verification
        if next_sel:
            try:
                el = soup.select_one(next_sel)
                if not el:
                    warnings.append(f"next_selector {next_sel!r} không tìm thấy element")
                elif el.name != "a" and not el.find("a"):
                    warnings.append(
                        f"next_selector {next_sel!r} → {el.name} tag, không có href"
                    )
            except Exception as e:
                warnings.append(f"next_selector lỗi: {e}")

        break  # chỉ kiểm tra 1 chapter đầu tiên thôi (các chạy đánh giá sau nếu cần)

    profile["uncertain_fields"] = uncertain
    return profile, warnings


async def run_learning_phase(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
) -> tuple[SiteProfile, list[str], list[tuple[str, str]]] | None:
    """
    Chạy 10-Chapter Deep Learning Phase.

    Returns:
        (profile, sample_raw_titles, fetched_chapters)
        fetched_chapters = list of (url, html) — tái dùng cho scraping,
        không cần fetch lại.
        None nếu thất bại.
    """
    from core.scraper import _dtag
    domain = urlparse(start_url).netloc.lower()
    tag    = _dtag(domain)

    print(f"\n{'═'*62}", flush=True)
    print(f"  🎓 Deep Learning: {domain}", flush=True)
    print(f"  📚 Fetching {LEARNING_CHAPTERS} chapters...", flush=True)
    print(f"{'═'*62}", flush=True)

    # ── Fetch 10 chapters ─────────────────────────────────────────────────────
    chapters = await _fetch_chapters(start_url, pool, pw_pool, pm, ai_limiter, domain)

    if len(chapters) < 4:
        print(
            f"  [{tag}] ✗ Chỉ fetch được {len(chapters)}/{LEARNING_CHAPTERS} chapters "
            f"— không đủ để học.",
            flush=True,
        )
        return None

    n = len(chapters)
    print(f"  [{tag}] ✓ Fetched {n}/{LEARNING_CHAPTERS} chapters\n", flush=True)

    # ── Content Validation Gate ─────────────────────────────────────────────────────
    force_playwright = False
    is_rich, reason = _validate_content_richness(chapters)
    if not is_rich:
        print(f"  [{tag}] ⚠ Content validation FAIL: {reason}", flush=True)
        print(f"  [{tag}] 🔄 Re-fetching tất cả chapters bằng Playwright...", flush=True)
        chapters = await _retry_with_playwright(chapters, pw_pool, domain, pool)
        force_playwright = True
        # Kiểm tra lại sau khi re-fetch
        is_rich2, reason2 = _validate_content_richness(chapters)
        if is_rich2:
            print(f"  [{tag}] ✅ Playwright giải quyết: {reason2}", flush=True)
        else:
            print(
                f"  [{tag}] ⚠ Vẫn thiếu nội dung sau PW: {reason2} — tiếp tục học...",
                flush=True,
            )

    # ── Run 10 AI calls ───────────────────────────────────────────────────────────────────
    profile = await _run_10_ai_calls(chapters, domain, ai_limiter)
    if profile is None:
        return None

    # Ghi force_playwright vào profile nếu cần
    if force_playwright and not profile.get("requires_playwright"):
        profile["requires_playwright"] = True
        print(f"  [{tag}] 🔒 force requires_playwright=True (JS-rendered site)", flush=True)

    # ── Selector Verification (post-learning) ─────────────────────────────────────────
    profile, sel_warnings = _verify_profile_selectors(profile, chapters)
    if sel_warnings:
        print(f"  [{tag}] ⚠ Selector verification warnings:", flush=True)
        for w in sel_warnings:
            print(f"     • {w}", flush=True)

    await pm.save_profile(domain, profile)

    fr = profile.get("formatting_rules") or {}
    print(
        f"\n  [{tag}] ✅ Profile saved!\n"
        f"     confidence        = {profile.get('confidence', 0):.2f}\n"
        f"     content_selector  = {profile.get('content_selector')!r}\n"
        f"     title_selector    = {profile.get('title_selector')!r}\n"
        f"     next_selector     = {profile.get('next_selector')!r}\n"
        f"     remove            = {profile.get('remove_selectors', [])}\n"
        f"     nav_type          = {profile.get('nav_type')!r}\n"
        f"     tables/math       = {fr.get('tables', False)} / {fr.get('math_support', False)}\n"
        f"     system_box        = {bool(fr.get('system_box', {}).get('found'))}\n"
        f"     ads_kw            = {len(profile.get('ads_keywords_learned', []))}",
        flush=True,
    )
    if profile.get("uncertain_fields"):
        print(
            f"     ⚠ uncertain: {profile['uncertain_fields']}",
            flush=True,
        )
    print(f"{'═'*62}\n", flush=True)

    # ── Extract raw titles cho Naming Phase ───────────────────────────────────
    from learning.naming import get_raw_title_from_html
    sample_titles: list[str] = [
        t for t in (get_raw_title_from_html(html) for _, html in chapters)
        if t
    ]

    return profile, sample_titles, chapters


# ── Chapter fetching ──────────────────────────────────────────────────────────

async def _fetch_chapters(
    start_url  : str,
    pool       : DomainSessionPool,
    pw_pool    : PlaywrightPool,
    pm         : ProfileManager,
    ai_limiter : AIRateLimiter,
    domain     : str,
) -> list[tuple[str, str]]:
    """Fetch LEARNING_CHAPTERS chapters. Returns list of (url, html)."""
    from core.scraper import _dtag
    tag = _dtag(domain)

    chapters: list[tuple[str, str]] = []
    current_url = start_url

    # Nếu start_url là trang Index → tìm Chapter 1 trước
    if not RE_CHAP_URL.search(start_url):
        print(f"  [{tag}] 📋 Index page → tìm Chapter 1...", flush=True)
        try:
            status, index_html = await pw_pool.fetch(start_url)
            if not is_junk_page(index_html, status):
                first_url = await ai_find_first_chapter(index_html, start_url, ai_limiter)
                if first_url and first_url != start_url:
                    print(f"  [{tag}] ✅ Chapter 1: {first_url[:65]}", flush=True)
                    current_url = first_url
                else:
                    print(f"  [{tag}] ⚠ Không tìm được Chapter 1", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [{tag}] ⚠ Index detection thất bại: {e}", flush=True)

    # Dùng empty profile để navigate (chưa có profile thật)
    temp_profile: SiteProfile = pm.get(domain)  # type: ignore[assignment]

    for i in range(LEARNING_CHAPTERS):
        if not current_url:
            break

        print(
            f"  [{tag}] Fetch Ch.{i+1:>2}/{LEARNING_CHAPTERS} → {current_url[:60]}",
            flush=True,
        )

        try:
            if i == 0:
                # Ch.1 dùng Playwright để đảm bảo full render
                status, html = await pw_pool.fetch(current_url)
            elif i == 1:
                # Ch.2: So sánh curl vs Playwright — phát hiện JS-rendered sites
                try:
                    status_curl, html_curl = await pool.fetch(current_url)
                except Exception:
                    status_curl, html_curl = 0, ""

                if _html_has_prose(html_curl):
                    # curl có prose → dùng hợp lệ
                    status, html = status_curl, html_curl
                else:
                    # curl rỗng → thử Playwright
                    print(
                        f"  [{tag}] 🔍 Ch.2 curl rỗng — test Playwright...",
                        flush=True,
                    )
                    try:
                        status_pw, html_pw = await pw_pool.fetch(current_url)
                    except Exception:
                        status_pw, html_pw = 0, ""

                    if _html_has_prose(html_pw):
                        print(
                            f"  [{tag}] 🌐 JS-rendered site phát hiện! "
                            f"Chuyển sang Playwright cho Ch.3-10.",
                            flush=True,
                        )
                        pool.mark_cf_domain(domain)
                        status, html = status_pw, html_pw
                    else:
                        # Cả hai đều rỗng — dùng curl (Content Validation Gate sẽ xử lý sau)
                        status, html = status_curl, html_curl
            else:
                status, html = await fetch_page(current_url, pool, pw_pool)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"  [{tag}] ⚠ Fetch Ch.{i+1} thất bại: {e}", flush=True)
            break

        if is_junk_page(html, status):
            print(f"  [{tag}] ⚠ Ch.{i+1} junk page (status={status})", flush=True)
            break

        chapters.append((current_url, html))

        # Navigate tới chapter tiếp theo
        if i < LEARNING_CHAPTERS - 1:
            soup     = BeautifulSoup(html, "html.parser")
            next_url = find_next_url(soup, current_url, temp_profile)

            if not next_url:
                print(
                    f"  [{tag}] ⚠ Heuristic navigation thất bại Ch.{i+1} → AI fallback...",
                    flush=True,
                )
                try:
                    ai_nav = await ai_classify_and_find(html, current_url, ai_limiter)
                    if ai_nav:
                        next_url = ai_nav.get("next_url")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[%s] AI nav thất bại: %s", tag, e)

            if not next_url:
                print(f"  [{tag}] ⚠ Không tìm được next URL sau Ch.{i+1}", flush=True)
                break

            current_url = next_url
            await asyncio.sleep(get_delay(current_url))

    return chapters


# ── 10 AI calls orchestration ─────────────────────────────────────────────────

async def _run_10_ai_calls(
    chapters   : list[tuple[str, str]],
    domain     : str,
    ai_limiter : AIRateLimiter,
) -> SiteProfile | None:
    """
    Orchestrate 10 AI calls theo 5 phases.
    Trả về SiteProfile đã tổng hợp, hoặc None nếu thất bại nghiêm trọng.
    """
    urls  = [url  for url,  _ in chapters]
    htmls = [html for _, html in chapters]
    n     = len(chapters)

    # Tracking toàn bộ results để build synthesis summary
    all_results: dict[str, dict | None] = {}

    # Accumulated dangerous selectors — KHÔNG BAO GIỜ được add vào remove list
    dangerous_selectors: set[str] = set()

    # ── PHASE 1: Structure Discovery ──────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 1: Structure Discovery ━━", flush=True)

    print(f"  [Learn] 🤖 AI#1: DOM structure mapping (Ch.1+2)...", flush=True)
    ai1 = await ai_dom_structure(
        htmls[0], urls[0],
        htmls[1], urls[1],
        ai_limiter,
    )
    all_results["ai1"] = ai1
    if ai1:
        print(
            f"     → content={ai1.get('content_selector')!r} "
            f"title={ai1.get('chapter_title_selector')!r} "
            f"next={ai1.get('next_selector')!r}",
            flush=True,
        )
        if ai1.get("title_is_inside_remove_candidate"):
            print(
                f"     ⚠ title_is_inside_remove_candidate=True "
                f"→ container={ai1.get('title_container')!r}",
                flush=True,
            )
    else:
        print(f"  [Learn] ⚠ AI#1 thất bại", flush=True)

    print(f"  [Learn] 🤖 AI#2: Independent cross-check (Ch.1+2)...", flush=True)
    ai2 = await ai_independent_check(
        htmls[0], urls[0],
        htmls[1], urls[1],
        ai_limiter,
    )
    all_results["ai2"] = ai2
    if ai2:
        print(
            f"     → content={ai2.get('content_selector')!r} "
            f"title={ai2.get('chapter_title_selector')!r} "
            f"conf={ai2.get('confidence', 0):.2f}",
            flush=True,
        )
        if ai2.get("uncertain_fields"):
            print(f"     ⚠ uncertain: {ai2['uncertain_fields']}", flush=True)

    # Resolve conflicts giữa AI#1 và AI#2
    consensus, p1_conflicts = resolve_phase1_conflicts(ai1, ai2)
    if p1_conflicts:
        print(
            f"  [Learn] ⚠ {len(p1_conflicts)} conflicts Phase 1: {p1_conflicts}",
            flush=True,
        )
    else:
        print(f"  [Learn] ✓ Phase 1: AI#1 và AI#2 đồng thuận", flush=True)

    if n >= 4:
        print(f"  [Learn] 🤖 AI#3: Selector stability (Ch.3+4)...", flush=True)
        ai3 = await ai_stability_check(
            htmls[2], urls[2],
            htmls[3], urls[3],
            consensus,
            ai_limiter,
        )
        all_results["ai3"] = ai3
        if ai3:
            score = ai3.get("stability_score", 0)
            print(f"     → stability_score={score:.2f}", flush=True)

            # Apply AI#3 fixes
            if ai3.get("content_fix"):
                consensus["content_selector"] = ai3["content_fix"]
                print(f"     → content fix: {ai3['content_fix']!r}", flush=True)
            if ai3.get("title_fix"):
                consensus["chapter_title_selector"] = ai3["title_fix"]
                print(f"     → title fix: {ai3['title_fix']!r}", flush=True)
            if ai3.get("next_fix"):
                consensus["next_selector"] = ai3["next_fix"]
                print(f"     → next fix: {ai3['next_fix']!r}", flush=True)

            # Cập nhật remove list: chỉ giữ selectors đã verify safe
            safe_rm    = set(ai3.get("remove_selectors_safe",      []))
            dangerous  = set(ai3.get("remove_selectors_dangerous", []))
            add_rm     = set(ai3.get("remove_add",                 []))

            dangerous_selectors.update(dangerous)
            if dangerous:
                print(f"     ⚠ Dangerous selectors detected: {list(dangerous)}", flush=True)

            # Remove list = safe selectors + new adds - dangerous
            current_rm = set(consensus.get("remove_selectors") or [])
            new_rm = (current_rm & safe_rm) | add_rm - dangerous_selectors
            consensus["remove_selectors"] = list(new_rm)
        else:
            print(f"  [Learn] ⚠ AI#3 thất bại — giữ consensus Phase 1", flush=True)
    else:
        all_results["ai3"] = None

    # ── PHASE 2: Conflict Resolution ──────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 2: Conflict Resolution ━━", flush=True)

    current_remove = list(consensus.get("remove_selectors") or [])

    if n >= 5 and current_remove:
        print(f"  [Learn] 🤖 AI#4: Remove selectors audit (Ch.5)...", flush=True)
        ai4 = await ai_remove_audit(
            htmls[4], urls[4],
            current_remove,
            consensus.get("content_selector"),
            consensus.get("chapter_title_selector"),
            ai_limiter,
        )
        all_results["ai4"] = ai4
        if ai4:
            newly_dangerous = set(ai4.get("dangerous_selectors") or [])
            dangerous_selectors.update(newly_dangerous)

            if newly_dangerous:
                print(
                    f"     ⚠ {len(newly_dangerous)} dangerous selectors removed: "
                    f"{list(newly_dangerous)}",
                    flush=True,
                )

            # Apply safe selectors từ audit
            safe = set(ai4.get("safe_selectors") or [])
            # Giữ lại safe selectors, loại bỏ dangerous
            consensus["remove_selectors"] = [
                s for s in current_remove
                if s not in dangerous_selectors
            ]
            print(
                f"     → {len(consensus['remove_selectors'])} safe remove selectors",
                flush=True,
            )
        else:
            print(f"  [Learn] ⚠ AI#4 thất bại — giữ remove list hiện tại", flush=True)
    elif not current_remove:
        print(f"  [Learn] ℹ AI#4: skip (remove list trống)", flush=True)
        all_results["ai4"] = None
    else:
        all_results["ai4"] = None

    if n >= 6:
        print(f"  [Learn] 🤖 AI#5: Title deep-dive (Ch.6)...", flush=True)
        ai5 = await ai_title_deepdive(
            htmls[5], urls[5],
            consensus.get("chapter_title_selector"),
            (ai1 or {}).get("author_selector") or (ai2 or {}).get("author_selector"),
            ai_limiter,
        )
        all_results["ai5"] = ai5
        if ai5:
            if ai5.get("author_contamination_risk"):
                print(
                    f"     ⚠ Author contamination risk detected! "
                    f"author={ai5.get('author_name_detected')!r}",
                    flush=True,
                )
            best = ai5.get("recommended_title_selector") or ai5.get("best_title_selector")
            if best and best != consensus.get("chapter_title_selector"):
                print(
                    f"     → title selector refined: "
                    f"{consensus.get('chapter_title_selector')!r} → {best!r}",
                    flush=True,
                )
                consensus["chapter_title_selector"] = best
            else:
                print(f"     → title selector confirmed ✓", flush=True)
        else:
            print(f"  [Learn] ⚠ AI#5 thất bại — giữ title selector hiện tại", flush=True)
    else:
        all_results["ai5"] = None

    # ── PHASE 3: Content Intelligence ─────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 3: Content Intelligence ━━", flush=True)

    formatting_rules: dict = {}

    if n >= 7:
        print(f"  [Learn] 🤖 AI#6: Special content detection (Ch.7)...", flush=True)
        ai6 = await ai_special_content(htmls[6], urls[6], ai_limiter)
        all_results["ai6"] = ai6
        if ai6:
            formatting_rules["tables"]         = ai6.get("has_tables", False)
            formatting_rules["math_support"]   = ai6.get("has_math", False)
            formatting_rules["math_format"]    = ai6.get("math_format")
            formatting_rules["special_symbols"]= ai6.get("special_symbols", [])
            formatting_rules["bold_italic"]    = ai6.get("bold_italic", True)
            formatting_rules["hr_dividers"]    = ai6.get("hr_dividers", True)
            formatting_rules["image_alt_text"] = ai6.get("image_alt_text", False)
            for key in ("system_box", "hidden_text", "author_note"):
                formatting_rules[key] = ai6.get(key, {"found": False, "selectors": []})

            # HTML scan override cho tables
            if not formatting_rules["tables"]:
                if any("<table" in h.lower() for h in htmls):
                    formatting_rules["tables"] = True
                    print(f"     → Tables: HTML scan override → bật flag", flush=True)
        else:
            print(f"  [Learn] ⚠ AI#6 thất bại — HTML scan fallback", flush=True)
            formatting_rules["tables"]       = any("<table" in h.lower() for h in htmls)
            formatting_rules["math_support"] = False
    else:
        all_results["ai6"] = None

    ads_keywords: list[str] = []

    if n >= 8:
        print(f"  [Learn] 🤖 AI#7: Ads deep scan (Ch.8)...", flush=True)
        ai7 = await ai_ads_deepscan(htmls[7], urls[7], ai_limiter)
        all_results["ai7"] = ai7
        if ai7:
            ads_keywords = list(ai7.get("ads_keywords") or [])
            # Ads selectors từ AI#7 có thể add vào remove list (đã verify safe)
            ads_sels = [
                s for s in (ai7.get("ads_selectors") or [])
                if s not in dangerous_selectors
            ]
            if ads_sels:
                existing_rm = set(consensus.get("remove_selectors") or [])
                existing_rm.update(ads_sels)
                consensus["remove_selectors"] = list(existing_rm)
                print(f"     → +{len(ads_sels)} ads selectors added", flush=True)
            if ads_keywords:
                print(f"     → {len(ads_keywords)} ads keywords", flush=True)
        else:
            print(f"  [Learn] ⚠ AI#7 thất bại", flush=True)
    else:
        all_results["ai7"] = None

    # ── PHASE 4: Stress Test ──────────────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 4: Stress Test ━━", flush=True)

    if n >= 9:
        print(f"  [Learn] 🤖 AI#8: Navigation stress test (Ch.9)...", flush=True)
        ai8 = await ai_nav_stress(
            htmls[8], urls[8],
            consensus.get("next_selector"),
            consensus.get("nav_type"),
            ai_limiter,
        )
        all_results["ai8"] = ai8
        if ai8:
            if not ai8.get("next_selector_works") and ai8.get("best_next_selector"):
                consensus["next_selector"] = ai8["best_next_selector"]
                print(f"     → next_selector fixed: {ai8['best_next_selector']!r}", flush=True)
            if ai8.get("nav_type_confirmed"):
                consensus["nav_type"] = ai8["nav_type_confirmed"]
            if ai8.get("chapter_url_pattern_fix"):
                consensus["chapter_url_pattern"] = ai8["chapter_url_pattern_fix"]
            print(
                f"     → nav={consensus.get('nav_type')!r} "
                f"works={ai8.get('next_selector_works')}",
                flush=True,
            )
        else:
            print(f"  [Learn] ⚠ AI#8 thất bại", flush=True)
    else:
        all_results["ai8"] = None

    # Build profile_so_far cho simulation
    profile_so_far = {
        "content_selector"      : consensus.get("content_selector"),
        "chapter_title_selector": consensus.get("chapter_title_selector"),
        "next_selector"         : consensus.get("next_selector"),
        "remove_selectors"      : consensus.get("remove_selectors", []),
        "nav_type"              : consensus.get("nav_type"),
    }

    if n >= 10:
        print(f"  [Learn] 🤖 AI#9: Full profile simulation (Ch.10)...", flush=True)
        ai9 = await ai_full_simulation(
            htmls[9], urls[9],
            profile_so_far,
            ai_limiter,
        )
        all_results["ai9"] = ai9
        if ai9:
            score = ai9.get("overall_score", 0)
            issues = ai9.get("issues_found") or []
            print(f"     → overall_score={score:.2f}", flush=True)
            if issues:
                for issue in issues[:3]:
                    print(f"     ⚠ {issue}", flush=True)
            if not ai9.get("removal_safe", True):
                print(
                    f"     ⚠ Simulation: removal NOT safe — reverting to empty remove list",
                    flush=True,
                )
                consensus["remove_selectors"] = []
        else:
            print(f"  [Learn] ⚠ AI#9 thất bại", flush=True)
    else:
        all_results["ai9"] = None

    # ── PHASE 5: Master Synthesis ─────────────────────────────────────────────
    print(f"\n  [Learn] ━━ Phase 5: Master Synthesis ━━", flush=True)
    print(f"  [Learn] 🤖 AI#10: Master profile synthesis...", flush=True)

    synthesis_summary = _build_synthesis_summary(
        all_results, consensus, dangerous_selectors,
        ads_keywords, formatting_rules, n,
    )

    ai10 = await ai_master_synthesis(synthesis_summary, domain, ai_limiter)
    all_results["ai10"] = ai10

    # Build final profile
    if ai10:
        print(
            f"     → confidence={ai10.get('confidence', 0):.2f} "
            f"uncertain={ai10.get('uncertain_fields', [])}",
            flush=True,
        )
        if ai10.get("conflict_summary"):
            print(f"     → conflicts: {ai10['conflict_summary'][:80]}", flush=True)

        # Safety net: loại bỏ bất kỳ dangerous selector nào từ AI#10
        final_remove = [
            s for s in (ai10.get("remove_selectors") or [])
            if s not in dangerous_selectors
        ]

        # Merge ads keywords — filter qua blacklist trước khi ghi vào profile
        from utils.ads_filter import _GENERIC_KEYWORD_BLACKLIST, _is_rpg_story_content
        raw_ads = list({
            *ads_keywords,
            *(ai10.get("ads_keywords") or []),
        })
        final_ads = [
            kw for kw in raw_ads
            if kw.lower().strip() not in _GENERIC_KEYWORD_BLACKLIST
            and len(kw.strip()) >= 8
            and not _is_rpg_story_content(kw)
        ]

        # title_selector: ưu tiên AI#10 → consensus → fallback
        final_title = (
            ai10.get("chapter_title_selector")
            or consensus.get("chapter_title_selector")
        )

        profile: SiteProfile = {
            "domain"             : domain,
            "last_learned"       : _now_iso(),
            "confidence"         : ai10.get("confidence", 0.7),
            "content_selector"   : ai10.get("content_selector") or consensus.get("content_selector"),
            "next_selector"      : ai10.get("next_selector")    or consensus.get("next_selector"),
            "title_selector"     : final_title,
            "remove_selectors"   : final_remove,
            "nav_type"           : ai10.get("nav_type")         or consensus.get("nav_type"),
            "chapter_url_pattern": ai10.get("chapter_url_pattern") or consensus.get("chapter_url_pattern"),
            "requires_playwright": bool(
                ai10.get("requires_playwright", False) or
                consensus.get("requires_playwright", False)
            ),
            "formatting_rules"   : ai10.get("formatting_rules") or formatting_rules,
            "ads_keywords_learned": final_ads,
            "learned_chapters"   : list(range(1, n + 1)),
            "sample_urls"        : urls,
            "uncertain_fields"   : ai10.get("uncertain_fields", []),
            "learning_version"   : 2,  # v2 = 10-call system
        }
    else:
        # AI#10 thất bại → dùng consensus
        print(
            f"  [Learn] ⚠ AI#10 thất bại — dùng consensus từ Phase 1-4",
            flush=True,
        )
        final_remove = [
            s for s in (consensus.get("remove_selectors") or [])
            if s not in dangerous_selectors
        ]
        confidence = _estimate_confidence(all_results, n)

        profile = {
            "domain"             : domain,
            "last_learned"       : _now_iso(),
            "confidence"         : confidence,
            "content_selector"   : consensus.get("content_selector"),
            "next_selector"      : consensus.get("next_selector"),
            "title_selector"     : consensus.get("chapter_title_selector"),
            "remove_selectors"   : final_remove,
            "nav_type"           : consensus.get("nav_type"),
            "chapter_url_pattern": consensus.get("chapter_url_pattern"),
            "requires_playwright": bool(consensus.get("requires_playwright", False)),
            "formatting_rules"   : formatting_rules,
            "ads_keywords_learned": ads_keywords,
            "learned_chapters"   : list(range(1, n + 1)),
            "sample_urls"        : urls,
            "uncertain_fields"   : [],
            "learning_version"   : 2,
        }

    return profile  # type: ignore[return-value]


# ── Synthesis summary builder ─────────────────────────────────────────────────

def _build_synthesis_summary(
    results           : dict[str, dict | None],
    consensus         : dict,
    dangerous_selectors: set[str],
    ads_keywords      : list[str],
    formatting_rules  : dict,
    n_chapters        : int,
) -> str:
    """Build text summary từ 9 AI results để gửi cho AI#10."""
    lines: list[str] = []

    lines.append(f"Chapters fetched: {n_chapters}")
    lines.append(f"\n--- PHASE 1 CONSENSUS ---")
    lines.append(f"content_selector      : {consensus.get('content_selector')!r}")
    lines.append(f"chapter_title_selector: {consensus.get('chapter_title_selector')!r}")
    lines.append(f"next_selector         : {consensus.get('next_selector')!r}")
    lines.append(f"nav_type              : {consensus.get('nav_type')!r}")
    lines.append(f"chapter_url_pattern   : {consensus.get('chapter_url_pattern')!r}")
    lines.append(f"remove_selectors      : {consensus.get('remove_selectors', [])}")
    lines.append(f"requires_playwright   : {consensus.get('requires_playwright', False)}")

    # Phase 1 details
    ai1 = results.get("ai1") or {}
    ai2 = results.get("ai2") or {}
    lines.append(f"\n--- AI#1 RESULTS ---")
    lines.append(f"author_selector     : {ai1.get('author_selector')!r}")
    lines.append(f"title_in_remove_cand: {ai1.get('title_is_inside_remove_candidate', False)}")
    lines.append(f"title_container     : {ai1.get('title_container')!r}")
    lines.append(f"\n--- AI#2 RESULTS ---")
    lines.append(f"confidence      : {ai2.get('confidence', 0):.2f}")
    lines.append(f"uncertain_fields: {ai2.get('uncertain_fields', [])}")
    lines.append(f"author_selector : {ai2.get('author_selector')!r}")

    # Dangerous selectors — CRITICAL
    if dangerous_selectors:
        lines.append(f"\n--- ⚠ DANGEROUS SELECTORS (NEVER ADD TO REMOVE LIST) ---")
        for s in sorted(dangerous_selectors):
            lines.append(f"  DANGEROUS: {s!r}")

    # Phase 2
    ai4 = results.get("ai4") or {}
    ai5 = results.get("ai5") or {}
    if ai4:
        lines.append(f"\n--- AI#4 REMOVE AUDIT ---")
        lines.append(f"safe     : {ai4.get('safe_selectors', [])}")
        lines.append(f"dangerous: {ai4.get('dangerous_selectors', [])}")
    if ai5:
        lines.append(f"\n--- AI#5 TITLE DEEP-DIVE ---")
        lines.append(f"recommended_title_selector: {ai5.get('recommended_title_selector')!r}")
        lines.append(f"author_contamination_risk : {ai5.get('author_contamination_risk', False)}")
        lines.append(f"author_name_detected      : {ai5.get('author_name_detected')!r}")

    # Phase 3
    ai6 = results.get("ai6") or {}
    ai7 = results.get("ai7") or {}
    if ai6:
        lines.append(f"\n--- AI#6 SPECIAL CONTENT ---")
        lines.append(f"tables     : {ai6.get('has_tables', False)}")
        lines.append(f"math       : {ai6.get('has_math', False)}")
        lines.append(f"system_box : {ai6.get('system_box', {}).get('found', False)}")
        lines.append(f"author_note: {ai6.get('author_note', {}).get('found', False)}")
    if ai7:
        lines.append(f"\n--- AI#7 ADS SCAN ---")
        lines.append(f"ads_keywords: {ai7.get('ads_keywords', [])[:10]}")
    elif ads_keywords:
        lines.append(f"\n--- ADS KEYWORDS (accumulated) ---")
        lines.append(f"{ads_keywords[:10]}")

    # Phase 4
    ai8 = results.get("ai8") or {}
    ai9 = results.get("ai9") or {}
    if ai8:
        lines.append(f"\n--- AI#8 NAV STRESS ---")
        lines.append(f"next_selector_works: {ai8.get('next_selector_works')}")
        lines.append(f"nav_type_confirmed : {ai8.get('nav_type_confirmed')!r}")
    if ai9:
        lines.append(f"\n--- AI#9 SIMULATION ---")
        lines.append(f"overall_score : {ai9.get('overall_score', 0):.2f}")
        lines.append(f"title_extracted: {ai9.get('title_extracted')!r}")
        lines.append(f"content_chars  : {ai9.get('content_char_count', 0)}")
        lines.append(f"removal_safe   : {ai9.get('removal_safe', True)}")
        if ai9.get("issues_found"):
            lines.append(f"issues         : {ai9['issues_found']}")

    # Formatting rules summary
    if formatting_rules:
        lines.append(f"\n--- FORMATTING RULES ---")
        lines.append(f"tables    : {formatting_rules.get('tables', False)}")
        lines.append(f"math      : {formatting_rules.get('math_support', False)}")
        lines.append(f"bold_italic: {formatting_rules.get('bold_italic', True)}")

    return "\n".join(lines)


def _estimate_confidence(results: dict, n_chapters: int) -> float:
    """Estimate confidence từ kết quả các AI calls khi AI#10 thất bại."""
    scores: list[float] = []

    ai3 = results.get("ai3") or {}
    if ai3.get("stability_score"):
        scores.append(float(ai3["stability_score"]))

    ai9 = results.get("ai9") or {}
    if ai9.get("overall_score"):
        scores.append(float(ai9["overall_score"]))

    ai2 = results.get("ai2") or {}
    if ai2.get("confidence"):
        scores.append(float(ai2["confidence"]))

    base = 0.5 + 0.03 * min(n_chapters, 10)
    if scores:
        return round(min(sum(scores) / len(scores), 0.92), 2)
    return round(base, 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()