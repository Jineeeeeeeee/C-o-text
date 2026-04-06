"""
learning/optimizer.py — Pipeline optimization engine.

PipelineGenerator:
    Sinh ra N candidate PipelineConfig từ HTML đã fetch.
    Dùng heuristics để xác định strategies phù hợp:
        - Detect JS-heavy → ưu tiên Playwright fetcher
        - Detect rel=next → ưu tiên RelNextNavBlock
        - Detect <select> dropdown → thêm SelectDropdownNavBlock
        - Detect JSON-LD → ưu tiên JsonLdExtractBlock
        - v.v.
    Sinh tối đa MAX_CANDIDATES candidates (default 8).

PipelineEvaluator:
    Chạy từng candidate pipeline trên N cached HTML chapters.
    Tính score = 0.4*quality + 0.3*speed + 0.2*resource + 0.1*confidence
    Chọn winner. Lưu vào SiteProfile.

run_optimizer():
    Entry point. Nhận fetched_chapters, trả về PipelineConfig tốt nhất.

Fix H1: PipelineEvaluator._eval_one() truyền ai_limiter=None vào runner.run().
    Lý do: evaluator chạy 8 candidates × 5 chapters = 40 pipeline executions.
    Nếu truyền ai_limiter thật, AINavBlock / AIExtractBlock sẽ gọi Gemini
    trong mỗi lần eval — ăn mất quota của 10 AI calls đang học.
    AINavBlock và AIExtractBlock đã có guard sẵn:
        if ai_limiter is None: return BlockResult.skipped(...)
    Nên chỉ cần truyền None — các block đó tự skip gracefully.
    Heuristic blocks (selector, density, rel_next, anchor_text, v.v.)
    không dùng ai_limiter nên không bị ảnh hưởng.
"""
from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from pipeline.base import (
    ChainConfig, PipelineConfig, PipelineContext, StepConfig,
)
from pipeline.executor import ChainExecutor, PipelineRunner

logger = logging.getLogger(__name__)

MAX_CANDIDATES  = 8
MIN_CHAPTERS_TO_EVAL = 3   # Tối thiểu chapters để chấm điểm
_CONTENT_JS_RATIO    = 1.5  # Ngưỡng JS-heavy detection


# ── Candidate score record ────────────────────────────────────────────────────

@dataclass
class CandidateResult:
    """Kết quả đánh giá một candidate pipeline."""
    config        : PipelineConfig
    total_score   : float = 0.0
    quality_score : float = 0.0
    speed_score   : float = 0.0
    resource_score: float = 0.0
    confidence    : float = 0.0
    chapters_ok   : int   = 0
    chapters_total: int   = 0
    errors        : list  = field(default_factory=list)
    notes         : str   = ""


# ── HTML Analysis Helpers ─────────────────────────────────────────────────────

def _has_json_ld(soup: BeautifulSoup) -> bool:
    """Kiểm tra page có JSON-LD Article schema không."""
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(s.get_text(strip=True) or "{}")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") in ("Article", "BlogPosting", "NewsArticle"):
                    if item.get("articleBody"):
                        return True
        except Exception:
            pass
    return False


def _has_rel_next(soup: BeautifulSoup) -> bool:
    return bool(
        soup.find("link", rel="next") or soup.find("a", rel="next")
    )


def _has_select_dropdown(soup: BeautifulSoup) -> str | None:
    """Trả về selector nếu có chapter dropdown, None nếu không."""
    for sel in [
        "select#chapterList", "select.chapter-select",
        "select[name='chapter']", "select.selectpicker",
        "select#chapter",
    ]:
        el = soup.select_one(sel)
        if el and len(el.find_all("option")) > 2:
            return sel
    return None


def _find_content_selectors(soup: BeautifulSoup) -> list[str]:
    """Tìm các CSS selectors candidate cho content area."""
    candidates: list[str] = []
    for el in soup.find_all(["div", "article", "section"]):
        el_id = el.get("id", "")
        if el_id and len(el.get_text(strip=True)) > 300:
            candidates.append(f"#{el_id}")
        classes = el.get("class") or []
        for cls in classes:
            if any(kw in cls.lower() for kw in ("chapter", "content", "text", "story", "read")):
                full_sel = f"{el.name}.{cls}"
                text_len = len(el.get_text(strip=True))
                if text_len > 300 and full_sel not in candidates:
                    candidates.append(full_sel)
    return candidates[:6]


def _find_next_selectors(soup: BeautifulSoup) -> list[str]:
    """Tìm các CSS selectors candidate cho Next button."""
    from config import RE_NEXT_BTN
    candidates: list[str] = []
    for a in soup.find_all("a", href=True):
        if not RE_NEXT_BTN.search(a.get_text(strip=True)):
            continue
        classes = a.get("class") or []
        el_id   = a.get("id", "")
        rel     = a.get("rel", [])
        if el_id:
            candidates.append(f"a#{el_id}")
        for cls in classes:
            sel = f"a.{cls}"
            if sel not in candidates:
                candidates.append(sel)
        if "next" in rel:
            candidates.append("a[rel='next']")
    return candidates[:4]


def _find_title_selectors(soup: BeautifulSoup) -> list[str]:
    """Tìm selector cho chapter title."""
    candidates: list[str] = []
    for tag in ("h1", "h2"):
        els = soup.find_all(tag)
        for el in els[:2]:
            classes = el.get("class") or []
            el_id   = el.get("id", "")
            if el_id:
                candidates.append(f"{tag}#{el_id}")
            for cls in classes:
                if any(kw in cls.lower() for kw in ("title", "chapter", "heading")):
                    candidates.append(f"{tag}.{cls}")
            if not candidates:
                candidates.append(tag)
    return candidates[:3]


# ── PipelineGenerator ─────────────────────────────────────────────────────────

class PipelineGenerator:
    """
    Sinh N candidate PipelineConfig từ HTML của fetched chapters.
    """

    def generate(
        self,
        domain        : str,
        chapters      : list[tuple[str, str]],
        curl_htmls    : list[str] | None = None,
    ) -> list[PipelineConfig]:
        if not chapters:
            return [PipelineConfig.default_for_domain(domain)]

        url1, html1 = chapters[0]
        soup1 = BeautifulSoup(html1, "html.parser")

        has_json_ld     = _has_json_ld(soup1)
        has_rel_next    = _has_rel_next(soup1)
        dropdown_sel    = _has_select_dropdown(soup1)
        content_sels    = _find_content_selectors(soup1)
        next_sels       = _find_next_selectors(soup1)
        title_sels      = _find_title_selectors(soup1)

        is_js_heavy = False
        if curl_htmls and curl_htmls[0]:
            curl_len = len(BeautifulSoup(curl_htmls[0], "html.parser").get_text())
            pw_len   = len(soup1.get_text())
            if pw_len > curl_len * _CONTENT_JS_RATIO and (pw_len - curl_len) > 500:
                is_js_heavy = True
                logger.info("[Generator] JS-heavy detected: curl=%d pw=%d", curl_len, pw_len)

        print(
            f"  [Optimizer] 🔍 Signals: json_ld={has_json_ld} rel_next={has_rel_next} "
            f"dropdown={bool(dropdown_sel)} js_heavy={is_js_heavy} "
            f"content_sels={len(content_sels)} next_sels={len(next_sels)}",
            flush=True,
        )

        candidates: list[PipelineConfig] = []

        focused = self._build_focused(
            domain, is_js_heavy, has_json_ld, has_rel_next,
            dropdown_sel, content_sels[:1], next_sels[:1], title_sels[:1],
        )
        candidates.append(focused)

        for i, cs in enumerate(content_sels[:3]):
            ns = next_sels[i] if i < len(next_sels) else None
            ts = title_sels[i] if i < len(title_sels) else None
            cand = self._build_selector_variant(
                domain, cs, ns, ts, is_js_heavy,
                label=f"selector_v{i+1}",
            )
            if not _config_duplicate(cand, candidates):
                candidates.append(cand)
            if len(candidates) >= MAX_CANDIDATES - 2:
                break

        if has_json_ld:
            jld = self._build_json_ld_focused(domain, is_js_heavy, next_sels)
            if not _config_duplicate(jld, candidates):
                candidates.append(jld)

        if dropdown_sel:
            dd = self._build_dropdown_variant(domain, is_js_heavy, dropdown_sel, content_sels)
            if not _config_duplicate(dd, candidates):
                candidates.append(dd)

        default = PipelineConfig.default_for_domain(domain)
        if not _config_duplicate(default, candidates):
            candidates.append(default)

        candidates = candidates[:MAX_CANDIDATES]
        print(
            f"  [Optimizer] 📋 Generated {len(candidates)} candidate pipelines",
            flush=True,
        )
        return candidates

    def _build_focused(
        self,
        domain, is_js_heavy, has_json_ld, has_rel_next,
        dropdown_sel, content_sels, next_sels, title_sels,
    ) -> PipelineConfig:
        fetch_steps = (
            [StepConfig("playwright"), StepConfig("hybrid")]
            if is_js_heavy
            else [StepConfig("hybrid"), StepConfig("playwright")]
        )

        extract_steps: list[StepConfig] = []
        if content_sels:
            extract_steps.append(StepConfig("selector", {"selector": content_sels[0]}))
        if has_json_ld:
            extract_steps.append(StepConfig("json_ld"))
        extract_steps += [
            StepConfig("density_heuristic"),
            StepConfig("fallback_list"),
        ]

        title_steps: list[StepConfig] = []
        if title_sels:
            title_steps.append(StepConfig("selector", {"selector": title_sels[0]}))
        title_steps += [
            StepConfig("h1_tag"),
            StepConfig("title_tag"),
            StepConfig("og_title"),
            StepConfig("url_slug"),
        ]

        nav_steps: list[StepConfig] = []
        if has_rel_next:
            nav_steps.append(StepConfig("rel_next"))
        if next_sels:
            nav_steps.append(StepConfig("selector", {"selector": next_sels[0]}))
        if dropdown_sel:
            nav_steps.append(StepConfig("select_dropdown", {"select_selector": dropdown_sel}))
        nav_steps += [
            StepConfig("anchor_text"),
            StepConfig("slug_increment"),
            StepConfig("fanfic"),
            StepConfig("ai_nav"),
        ]
        nav_steps = _dedup_steps(nav_steps)

        return PipelineConfig(
            domain        = domain,
            fetch_chain   = ChainConfig("fetch",    fetch_steps),
            extract_chain = ChainConfig("extract",  extract_steps),
            title_chain   = ChainConfig("title",    title_steps),
            nav_chain     = ChainConfig("navigate", nav_steps),
            validate_chain= ChainConfig("validate", [
                StepConfig("length",        {"min_chars": 100}),
                StepConfig("prose_richness",{"min_word_count": 20}),
            ]),
            notes = "focused_candidate",
        )

    def _build_selector_variant(
        self, domain, content_sel, next_sel, title_sel, is_js_heavy, label="",
    ) -> PipelineConfig:
        fetch_steps = (
            [StepConfig("playwright"), StepConfig("hybrid")]
            if is_js_heavy
            else [StepConfig("curl"), StepConfig("playwright")]
        )

        extract_steps = [
            StepConfig("selector", {"selector": content_sel}),
            StepConfig("density_heuristic"),
            StepConfig("fallback_list"),
        ]

        title_steps: list[StepConfig] = []
        if title_sel:
            title_steps.append(StepConfig("selector", {"selector": title_sel}))
        title_steps += [StepConfig("h1_tag"), StepConfig("title_tag"), StepConfig("og_title"), StepConfig("url_slug")]

        nav_steps: list[StepConfig] = [StepConfig("rel_next")]
        if next_sel:
            nav_steps.append(StepConfig("selector", {"selector": next_sel}))
        nav_steps += [StepConfig("anchor_text"), StepConfig("slug_increment"), StepConfig("fanfic"), StepConfig("ai_nav")]

        return PipelineConfig(
            domain        = domain,
            fetch_chain   = ChainConfig("fetch",    fetch_steps),
            extract_chain = ChainConfig("extract",  extract_steps),
            title_chain   = ChainConfig("title",    title_steps),
            nav_chain     = ChainConfig("navigate", nav_steps),
            validate_chain= ChainConfig("validate", [
                StepConfig("length",        {"min_chars": 100}),
                StepConfig("prose_richness",{"min_word_count": 20}),
            ]),
            notes = label,
        )

    def _build_json_ld_focused(self, domain, is_js_heavy, next_sels) -> PipelineConfig:
        fetch_steps = (
            [StepConfig("playwright")] if is_js_heavy
            else [StepConfig("curl"), StepConfig("playwright")]
        )
        nav_steps = [StepConfig("rel_next")]
        if next_sels:
            nav_steps.append(StepConfig("selector", {"selector": next_sels[0]}))
        nav_steps += [StepConfig("anchor_text"), StepConfig("slug_increment"), StepConfig("fanfic"), StepConfig("ai_nav")]

        return PipelineConfig(
            domain        = domain,
            fetch_chain   = ChainConfig("fetch", fetch_steps),
            extract_chain = ChainConfig("extract", [
                StepConfig("json_ld"),
                StepConfig("density_heuristic"),
                StepConfig("fallback_list"),
            ]),
            title_chain   = ChainConfig("title", [
                StepConfig("h1_tag"), StepConfig("title_tag"),
                StepConfig("og_title"), StepConfig("url_slug"),
            ]),
            nav_chain     = ChainConfig("navigate", nav_steps),
            validate_chain= ChainConfig("validate", [
                StepConfig("length",        {"min_chars": 100}),
                StepConfig("prose_richness",{"min_word_count": 20}),
            ]),
            notes = "json_ld_focused",
        )

    def _build_dropdown_variant(self, domain, is_js_heavy, dropdown_sel, content_sels) -> PipelineConfig:
        fetch_steps = (
            [StepConfig("playwright")] if is_js_heavy
            else [StepConfig("curl"), StepConfig("playwright")]
        )
        extract_steps: list[StepConfig] = []
        if content_sels:
            extract_steps.append(StepConfig("selector", {"selector": content_sels[0]}))
        extract_steps += [StepConfig("density_heuristic"), StepConfig("fallback_list")]

        return PipelineConfig(
            domain        = domain,
            fetch_chain   = ChainConfig("fetch", fetch_steps),
            extract_chain = ChainConfig("extract", extract_steps),
            title_chain   = ChainConfig("title", [
                StepConfig("h1_tag"), StepConfig("title_tag"),
                StepConfig("og_title"), StepConfig("url_slug"),
            ]),
            nav_chain     = ChainConfig("navigate", [
                StepConfig("select_dropdown", {"select_selector": dropdown_sel}),
                StepConfig("rel_next"),
                StepConfig("anchor_text"),
                StepConfig("slug_increment"),
                StepConfig("fanfic"),
                StepConfig("ai_nav"),
            ]),
            validate_chain= ChainConfig("validate", [
                StepConfig("length",        {"min_chars": 100}),
                StepConfig("prose_richness",{"min_word_count": 20}),
            ]),
            notes = "dropdown_nav",
        )


# ── PipelineEvaluator ─────────────────────────────────────────────────────────

class PipelineEvaluator:
    """
    Chạy từng candidate pipeline trên cached HTML chapters.
    Chấm điểm, chọn winner.

    Chạy TUẦN TỰ (không song song) để tránh race condition với shared pools.
    Learning phase chỉ chạy 1 lần → tốc độ không critical.
    """

    async def evaluate(
        self,
        candidates: list[PipelineConfig],
        chapters  : list[tuple[str, str]],
        profile   : dict,
        pool      : object,
        pw_pool   : object,
        ai_limiter: object,
    ) -> CandidateResult:
        if not candidates:
            raise ValueError("No candidates to evaluate")

        n_eval = max(MIN_CHAPTERS_TO_EVAL, min(len(chapters), 5))
        eval_chapters = chapters[:n_eval]

        print(
            f"  [Optimizer] ⚖️  Đánh giá {len(candidates)} candidates "
            f"trên {n_eval} chapters...",
            flush=True,
        )

        results: list[CandidateResult] = []
        for i, config in enumerate(candidates):
            result = await self._eval_one(
                config, eval_chapters, profile, pool, pw_pool,
                # Fix H1: truyền ai_limiter=None — AI blocks (AINavBlock,
                # AIExtractBlock) sẽ tự skip gracefully thay vì gọi Gemini.
                # Evaluation chỉ cần đo hiệu quả của heuristic blocks;
                # AI fallback không nên được tính vào score của pipeline.
                ai_limiter=None,
            )
            results.append(result)
            print(
                f"  [Optimizer]   [{i+1}/{len(candidates)}] "
                f"{config.notes or 'candidate':<20} "
                f"score={result.total_score:.3f} "
                f"ok={result.chapters_ok}/{result.chapters_total} "
                f"quality={result.quality_score:.2f}",
                flush=True,
            )

        winner = max(results, key=lambda r: (r.total_score, r.chapters_ok))
        print(
            f"  [Optimizer] 🏆 Winner: {winner.config.notes or 'candidate'} "
            f"score={winner.total_score:.3f}",
            flush=True,
        )
        return winner

    async def _eval_one(
        self,
        config    : PipelineConfig,
        chapters  : list[tuple[str, str]],
        profile   : dict,
        pool      : object,
        pw_pool   : object,
        ai_limiter: object,   # None khi gọi từ evaluate() — intentional
    ) -> CandidateResult:
        """Evaluate một candidate trên tất cả eval chapters."""
        runner       = PipelineRunner(config)
        total_scores : list[dict] = []
        errors       : list[str]  = []
        chapters_ok  : int        = 0

        for url, html in chapters:
            try:
                ctx = await runner.run(
                    url             = url,
                    profile         = profile,
                    progress        = {},
                    pool            = pool,
                    pw_pool         = pw_pool,
                    ai_limiter      = ai_limiter,   # None → AI blocks skip
                    prefetched_html = html,
                )

                if ctx.content and len(ctx.content.strip()) >= 100:
                    chapters_ok += 1
                    total_scores.append(ctx.get_pipeline_score())
                else:
                    errors.append(f"{url[:40]}: no content")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                errors.append(f"{url[:40]}: {str(e)[:60]}")

        if not total_scores:
            return CandidateResult(
                config         = config,
                chapters_total = len(chapters),
                errors         = errors,
                notes          = "all chapters failed",
            )

        avg = lambda key: sum(s[key] for s in total_scores) / len(total_scores)

        return CandidateResult(
            config         = config,
            total_score    = avg("total"),
            quality_score  = avg("quality"),
            speed_score    = avg("speed"),
            resource_score = avg("resource"),
            confidence     = avg("confidence"),
            chapters_ok    = chapters_ok,
            chapters_total = len(chapters),
            errors         = errors,
        )


# ── run_optimizer entry point ─────────────────────────────────────────────────

async def run_optimizer(
    domain           : str,
    chapters         : list[tuple[str, str]],
    existing_profile : dict,
    pool             : object,
    pw_pool          : object,
    ai_limiter       : object,
    curl_htmls       : list[str] | None = None,
) -> PipelineConfig:
    """
    Entry point cho learning/phase.py.

    1. PipelineGenerator sinh candidates
    2. PipelineEvaluator chọn winner (ai_limiter=None bên trong evaluator)
    3. Attach AI-learned selectors vào winner config
    4. Đặt timestamps + trả về
    """
    generator = PipelineGenerator()
    evaluator = PipelineEvaluator()

    candidates = generator.generate(
        domain     = domain,
        chapters   = chapters,
        curl_htmls = curl_htmls,
    )

    # ai_limiter được giữ lại ở đây để log nếu cần, nhưng
    # evaluator.evaluate() sẽ KHÔNG truyền xuống runner (truyền None thay thế).
    winner_result = await evaluator.evaluate(
        candidates = candidates,
        chapters   = chapters,
        profile    = existing_profile,
        pool       = pool,
        pw_pool    = pw_pool,
        ai_limiter = ai_limiter,  # evaluator nhận nhưng tự override = None khi chạy
    )

    winner = winner_result.config
    _merge_ai_selectors(winner, existing_profile)

    winner.score      = winner_result.total_score
    winner.created_at = datetime.now(timezone.utc).isoformat()
    winner.domain     = domain

    if winner_result.chapters_ok < MIN_CHAPTERS_TO_EVAL:
        winner.notes = (
            f"low_confidence: only {winner_result.chapters_ok}/"
            f"{winner_result.chapters_total} chapters ok"
        )
        logger.warning(
            "[Optimizer] low confidence for %s: %d/%d chapters ok",
            domain, winner_result.chapters_ok, winner_result.chapters_total,
        )

    return winner


def _merge_ai_selectors(config: PipelineConfig, profile: dict) -> None:
    """
    Merge AI-learned selectors từ profile vào pipeline config.
    AI selectors được thêm vào ĐẦU chain (highest priority) nếu chưa có.
    """
    ai_content = profile.get("content_selector")
    ai_next    = profile.get("next_selector")
    ai_title   = profile.get("title_selector")

    if ai_content:
        existing_types = {s.type for s in config.extract_chain.steps}
        if "selector" not in existing_types:
            config.extract_chain.steps.insert(
                0, StepConfig("selector", {"selector": ai_content})
            )
        else:
            for step in config.extract_chain.steps:
                if step.type == "selector" and not step.params.get("selector"):
                    step.params["selector"] = ai_content
                    break

    if ai_next:
        for step in config.nav_chain.steps:
            if step.type == "selector" and not step.params.get("selector"):
                step.params["selector"] = ai_next
                break
        else:
            insert_at = 1 if any(s.type == "rel_next" for s in config.nav_chain.steps) else 0
            config.nav_chain.steps.insert(
                insert_at, StepConfig("selector", {"selector": ai_next})
            )

    if ai_title:
        for step in config.title_chain.steps:
            if step.type == "selector" and not step.params.get("selector"):
                step.params["selector"] = ai_title
                break
        else:
            config.title_chain.steps.insert(
                0, StepConfig("selector", {"selector": ai_title})
            )


# ── Utilities ─────────────────────────────────────────────────────────────────

def _config_duplicate(new: PipelineConfig, existing: list[PipelineConfig]) -> bool:
    new_dict = new.to_dict()
    for e in existing:
        if e.to_dict() == new_dict:
            return True
    return False


def _dedup_steps(steps: list[StepConfig]) -> list[StepConfig]:
    seen: set[str] = set()
    result: list[StepConfig] = []
    for s in steps:
        key = str(s.to_dict())
        if key not in seen:
            seen.add(key)
            result.append(s)
    return result