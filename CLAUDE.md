# CLAUDE.md — Cào Text Project Knowledge Base

> **Last updated**: Based on full codebase review including all Batch fixes (P0–P3, M1–M7, L1–L4, C1–C3, EXT, FETCH, NAV, VAL, OPTIMIZER, ADS fixes).
> This file is the single source of truth for understanding what has been built, why decisions were made, and what conventions to follow.

---

## 1. Project Overview

**Cào Text** is an async Python web novel/fiction scraper built around a **"Lego blocks" philosophy**: the system thoroughly studies an unfamiliar site during a *learning phase*, then assembles a custom, site-specific scraping pipeline from composable, reusable blocks.

- **Target sites**: fanfiction.net, novelfire.net, royalroad.com, and similar fiction aggregators
- **Success criteria**: reliable, high-quality chapter extraction with minimal manual intervention per new domain
- **Runtime**: Python 3.8+ async (`asyncio`)
- **Platform**: Windows desktop (`C:\Users\FPT MONG CAI\Desktop\Small Project\Cào text`)

---

## 2. Architecture: The Two-Phase Model

```
Phase 1: LEARNING (per new domain, runs once)
  ┌─────────────────────────────────────────────────────────┐
  │  Fetch 10 chapters → 10 AI calls → Optimizer → Profile  │
  │  Persisted as SiteProfile to data/site_profiles.json    │
  └─────────────────────────────────────────────────────────┘

Phase 2: SCRAPING (concurrent, per story)
  ┌──────────────────────────────────────────────────────────┐
  │  Load Profile → Build PipelineRunner → Loop chapters:   │
  │  Fetch → Filter → Extract → Title → Navigate → Validate │
  └──────────────────────────────────────────────────────────┘
```

### 2.1 Pipeline Architecture (v2)

Each chapter scrape runs through a **5-chain pipeline**, where each chain is an ordered list of strategy *blocks* (first-wins, except title which uses weighted vote):

```
FetchChain     → ExtractChain → TitleChain → NavChain → ValidateChain
(get HTML)       (get text)     (get title)  (get URL)  (quality check)
```

**Key classes:**
| Class | File | Role |
|---|---|---|
| `PipelineRunner` | `pipeline/executor.py` | Executes config against a URL |
| `ChainExecutor` | `pipeline/executor.py` | Runs one chain; handles title_vote mode |
| `RuntimeContext` | `pipeline/base.py` | Non-serializable live objects (pools, limiter) |
| `BlockResult` | `pipeline/base.py` | Output of each block execution |

### 2.2 Serialization Format (CRITICAL — Fix M4)

`StepConfig` uses **nested `params`** format (v2). Do NOT use flat format (v1):

```python
# ✅ v2 (correct) — what to_dict() produces
{"type": "selector", "params": {"selector": "div.content"}}

# ❌ v1 (wrong, legacy) — DO NOT write this
{"type": "selector", "selector": "div.content"}
```

`StepConfig.from_dict()` handles both (backward compat via `from_legacy_dict()`).
`_make_block()` in `executor.py` flattens params: `cfg = {"type": ..., **params}` before calling factory.

---

## 3. Module Map

```
crawl_novel/
├── main.py                     # Entry point, CLI, AppState, 2-phase orchestration
├── config.py                   # All constants, regex, helpers — NO internal imports
│
├── ai/
│   ├── client.py               # Gemini client + AIRateLimiter (token bucket)
│   ├── agents.py               # All AI call functions + snippet() + _parse()
│   └── prompts.py              # PromptTemplates static class — all prompt strings
│
├── pipeline/
│   ├── base.py                 # Core types: BlockResult, PipelineContext, RuntimeContext,
│   │                           #   StepConfig, ChainConfig, PipelineConfig, ScraperBlock ABC
│   ├── context.py              # make_context() factory, context_summary()
│   ├── executor.py             # ChainExecutor, PipelineRunner, run_chapter()
│   ├── fetcher.py              # CurlFetchBlock, PlaywrightFetchBlock, HybridFetchBlock
│   ├── extractor.py            # SelectorExtract, JsonLd, DensityHeuristic, XPath,
│   │                           #   FallbackList, AIExtract blocks
│   ├── title_extractor.py      # SelectorTitle, H1Title, TitleTag, OgTitle, UrlSlug blocks
│   ├── navigator.py            # RelNext, Selector, AnchorText, SlugIncrement,
│   │                           #   Fanfic, SelectDropdown, AINav blocks
│   └── validator.py            # LengthValidator, ProseRichness, FingerprintDedup blocks
│
├── learning/
│   ├── phase.py                # run_learning_phase() orchestrator, _fetch_chapters(),
│   │                           #   _build_final_profile()
│   ├── phase_ai.py             # run_10_ai_calls_internal() — 10 AI call orchestration
│   ├── profile_manager.py      # ProfileManager — thread-safe profile CRUD
│   └── naming.py               # run_naming_phase() — story name + chapter keyword
│
├── core/
│   ├── scraper.py              # run_novel_task(), run_learning_only(),
│   │                           #   scrape_one_chapter(), _ensure_profile(),
│   │                           #   _setup_story(), _run_scrape_loop()
│   ├── fetch.py                # fetch_page() dispatcher (curl vs playwright)
│   ├── navigator.py            # find_next_url(), detect_page_type()
│   ├── html_filter.py          # prepare_soup() — 3-layer HTML filtering
│   ├── formatter.py            # MarkdownFormatter, extract_plain_text()
│   ├── extractor.py            # _title_from_url() — URL slug title fallback
│   ├── chapter_writer.py       # format_chapter_filename(), strip_nav_edges()
│   ├── story_meta.py           # extract_story_title(), build_story_id_regex(),
│   │                           #   is_chapter_url(), story_id_ok()
│   └── session_pool.py         # DomainSessionPool (curl_cffi), PlaywrightPool
│
└── utils/
    ├── types.py                # TypedDicts: SiteProfile, ProgressDict, AiResult types
    ├── string_helpers.py       # normalize_title, slugify_filename, is_junk_page,
    │                           #   is_cloudflare_challenge, make_fingerprint, domain_tag
    ├── file_io.py              # Async load/save for profiles, progress, markdown
    ├── ads_filter.py           # AdsFilter — per-domain ads keyword learning
    ├── content_cleaner.py      # clean_extracted_content() — 5-pass post-extraction cleaning
    └── issue_reporter.py       # IssueReporter — per-story issue tracking → issues.md
```

---

## 4. Key Data Structures

### 4.1 SiteProfile (utils/types.py)
Persisted in `data/site_profiles.json`, keyed by domain.

```python
{
    # Core identity
    "domain": "royalroad.com",
    "last_learned": "2024-01-01T00:00:00+00:00",
    "confidence": 0.95,
    "profile_version": 2,          # 1=legacy, 2=pipeline — KEY for migration

    # Legacy fields (v1, kept for backward compat)
    "content_selector": "div.chapter-content",
    "next_selector": "a.btn-next",
    "title_selector": "h1.chapter-title",
    "remove_selectors": [...],
    "nav_type": "selector",
    "chapter_url_pattern": r"/chapter/\d+",
    "requires_playwright": False,
    "formatting_rules": {...},
    "ads_keywords_learned": [...],

}
```

### 4.2 ProgressDict (utils/types.py)
Persisted per-story in `progress/{domain}_{slug}_{hash}.json`.

```python
{
    "current_url": "https://...",
    "chapter_count": 42,
    "story_title": "The Wandering Inn",
    "all_visited_urls": [...],
    "fingerprints": [...],
    "story_id_regex": r"/fiction/55418/",
    "story_id_locked": True,
    "completed": False,
    "learning_done": True,
    "start_url": "https://...",

    # Naming phase results
    "naming_done": True,
    "story_name_clean": "The Wandering Inn",
    "chapter_keyword": "Chapter",
    "has_chapter_subtitle": True,
    "story_prefix_strip": "",
    "output_dir_final": "output/The_Wandering_Inn",
}
```

### 4.3 RuntimeContext vs SiteProfile (CRITICAL SEPARATION)
```
RuntimeContext  → live objects (pools, ai_limiter) — NEVER serialize, NEVER in SiteProfile
SiteProfile     → learned structural data — always serializable JSON
ProgressDict    → per-story state — story-specific, not domain-specific
```

---

## 5. Learning Phase (8 AI Calls)

```
Phase 1 — Structure Discovery (Ch.1-4):
  AI#1 — Ch.1+2: Initial DOM structure mapping
  AI#2 — Ch.1+2: Independent cross-check (same data, independent)
  AI#3 — Ch.3+4: Selector stability validation

Phase 2 — Conflict Resolution (Ch.5-6):
  AI#4 — Ch.5: Remove selectors conflict audit
  AI#5 — Ch.6: Title extraction deep-dive + author contamination check

Phase 3 — Content Intelligence (Ch.7-8):
  AI#6 — Ch.7: Special content detection (tables/math/system boxes)
  AI#7 — Ch.8: Ads & watermark deep scan

Phase 4 — Master Synthesis
  AI#8 — Master profile synthesis
```

**Important conventions:**
- Learning agents (`phase_ai.py`) receive pre-trimmed HTML via `snippet()` — callers trim before calling
- Utility agents (`agents.py`) call `snippet()` themselves
- `_default_formatting_rules()` in `phase_ai.py` initializes full structure BEFORE AI#6 runs — if AI#6 fails, defaults are correct

---

## 6. HTML Filtering (3-Layer Defense)

`core/html_filter.py` — `prepare_soup()`:

1. **Layer 1**: Always remove `script, style, noscript, iframe`
2. **Layer 2**: `KNOWN_NOISE_SELECTORS` (hardcoded in `config.py`) — site-agnostic safety net
3. **Layer 3**: Profile `remove_selectors` — learned per-domain, with protection: won't remove ancestors of `content_selector` or `title_selector`

---

## 7. Content Cleaning (5-Pass Post-Extraction)

`utils/content_cleaner.py` — `clean_extracted_content()`:

```
Pass 0: _strip_raw_script_lines  (NEW — <script> text nodes, NovelFire-style injection)
Pass 1: _strip_comment_section   (from 30% of content downward)
Pass 2: _strip_settings_panel    (any position — RR reading settings)
Pass 3: _strip_postfix_section   (from 35% downward — nav/support footer)
Pass 4: _strip_metadata_header   (first 25 lines — FFN story stats)
Pass 5: _strip_author_bio        (from 55% downward — RR author section)
```

**Safety**: Never strips > 60% of original (`_MAX_STRIP_RATIO = 0.60`). Always returns original if cleaned < 40%.

---

## 8. Fetch Strategy

```
HybridFetchBlock (learning mode, detect_js=True):
  → Fetches BOTH curl AND playwright, compares text length
  → Reports js_heavy via BlockResult.metadata (NEVER mutates ctx.profile)
  → Executor sets ctx.detected_js_heavy = True
  → scraper.py persists to profile if needed

HybridFetchBlock (normal mode):
  → requires_playwright=True or CF domain → Playwright directly
  → Else: curl first → CF detected → Playwright + flag domain

FIX-STATUS: All fetch blocks pass status_code in metadata.
  Executor reads: ctx.status_code = fetch_result.metadata.get("status_code", 200)
```

---

## 9. Ads Filter System

Two-tier system in `utils/ads_filter.py`:

| Tier | Trigger | Action |
|---|---|---|
| Auto-add | frequency >= `_ADS_AUTO_THRESHOLD` (10) | Added without AI review |
| AI verify | `_ADS_AI_MIN_COUNT` (3) <= frequency < 10 | Sent to `ai_verify_ads()` |

**ADS-A Fix**: `scan_inline_for_watermarks()` scans the MIDDLE of content (skipping first/last 5 lines) across chapters. Inline suspects get **1.5× weight** vs edge suspects (stronger watermark signal). Tracking is per-file (each unique line counted once per chapter).

Persisted in `data/ads_keywords.json` keyed by domain. Injected into `AdsFilter` from profile at scrape start.

AdsFilter.save() uses _ADS_SAVE_LOCK (threading.Lock) + atomic write to prevent concurrent corruption.
_is_valid_ads_keyword() guards apply_verified() and inject_from_profile() — rejects HTML/script/URL strings.

---

## 10. Title Extraction

Title chain runs in **`title_vote` mode** — all blocks run, winner chosen by confidence-weighted vote with dash-normalized keys (`_make_vote_key`).

Priority order: `selector(0.95) → h1_tag(0.80) → title_tag(0.65) → og_title(0.65) → url_slug(0.40)`

**TITLE-1 Fix**: `strip_site_suffix()` matches `"FanFiction"` (without .net) AND strips FFN's `", a {fandom} fanfic"` descriptor in two passes.

strip_site_suffix() is now applied in ALL title blocks (SelectorTitle, H1Title), not just TitleTagBlock.

---

## 11. Chapter Filename Generation

`core/chapter_writer.py` — `format_chapter_filename()`:

```
"Chapter 23: Interlude 1"           → "0023_Interlude_1.md"
"Chapter 1, a percy jackson fanfic" → "0001_Chapter1.md"   (FILENAME-C: garbage subtitle guard)
"Chapter 23"                        → "0023_Chapter23.md"
"Prologue: The Beginning"           → "0001_Prologue_The_Beginning.md"
```

Uses `_get_chapter_re()` with `@lru_cache` (Fix P2-11 — hot path).
`_is_garbage_subtitle()` catches FFN descriptors, translator credits, and long artifact strings.

---

## 12. Critical Bugs Fixed (Reference)

### P0 — Critical
- **P0-2**: `ProfileManager.get()` returns shallow copy — never a live reference.
- **P0-4**: HTTP 429 removed from `_JUNK_STATUSES` — rate limit is temporary, not permanent error.
FIX-ADSSAVE: AdsFilter.save() concurrent write corruption (threading.Lock + atomic write + corrupt file recovery).
- Batch B: Xóa PipelineConfig serialization roundtrip — PipelineRunner đọc SiteProfile flat fields trực tiếp
### P1 — High
- **P1-A**: 429 with empty HTML raises `RuntimeError` (triggers retry), not silent `return None` (would terminate story).
- **P1-B**: `JS_CONTENT_RATIO` and `JS_MIN_DIFF_CHARS` in `config.py` — single source of truth.
- **P1-C**: `_default_formatting_rules()` initializes complete structure before AI#6 — if AI#6 fails, defaults still correct.
- **FIX-REQUIRESPW**: `requires_playwright` set from BOTH AI flag AND optimizer's fetch chain first step (`playwright` or `playwright_direct`).
- **FIX-STATUS**: All fetch blocks pass `status_code` in `BlockResult.metadata`.
- **FIX-CANCEL**: `asyncio.shield()` wraps `save_progress()` in `CancelledError` handler.
- **FIX-RATELEAK**: `AIRateLimiter.acquire()` rollbacks timestamp if cancelled during jitter sleep.
TITLE-B: H1TitleBlock applies strip_site_suffix() — strips [ ... words ] artifacts from h1 elements.
TITLE-A (extended): SelectorTitleBlock now applies strip_site_suffix() unconditionally (not just <title>).
PASS0-SCRIPT: content_cleaner Pass 0 strips <script> text nodes injected by sites like NovelFire.
FILENAME-E: format_chapter_filename() applies strip_site_suffix() to extracted subtitle.
### P2 — Medium
- **P2-11**: `_get_chapter_re()` uses `@lru_cache` in hot path.
- **P2-12**: Optimizer evaluates candidates in parallel via `asyncio.gather()`.
- **P2-A**: Title vote distinguishes SKIPPED vs FAILED in error messages.
- **P2-B**: `snippet()` early-exits if `len(html) <= max_len` (skip unnecessary BS4 parse).
- **P2-C**: `_parse()` uses `json.JSONDecoder.raw_decode()` instead of greedy regex (no ReDoS risk).
- **OPTIMIZER-A**: Edge noise penalty applied per-chapter before averaging in evaluator.

### P3 — Low
- **P3-18**: `os.makedirs()` called AFTER `actual_output_dir` is confirmed in `_setup_story()`.
- **FIX-REQUIRESPW**: Documented in `_build_final_profile()` with logger.info.

---

## 13. Important Conventions

### Never Do
```python
# ❌ Store runtime objects in profile
profile["_ai_limiter"] = ai_limiter

# ❌ Block mutates ctx.profile
ctx.profile["requires_playwright"] = True  # Use metadata signal instead

# ❌ read live profile reference
profile = pm.get(domain)
profile["x"] = y  # mutates internal state — always copy

# ❌ Catch CancelledError in generic except
try:
    await something()
except Exception as e:  # Wrong — CancelledError is BaseException
    pass

# ✅ Always catch CancelledError first
try:
    await something()
except asyncio.CancelledError:
    raise
except Exception as e:
    handle(e)
```

### Storage Separation
```
SiteProfile  → domain-level structural patterns (selectors, formatting rules)
ProgressDict → per-story state AND naming rules (story_name_clean, chapter_keyword, etc.)
RuntimeContext → live objects only, NEVER persisted
```

### Side Effects via Signals
Blocks must NOT mutate `ctx.profile` or `ctx.progress` directly.
Signal state changes via `BlockResult.metadata`. Example:
```python
# ✅ Correct — signal via metadata
return BlockResult.success(..., js_heavy=True)

# ❌ Wrong — block directly persisting state
ctx.profile["requires_playwright"] = True
```

### Profile Persistence
`ProfileManager` tracks `_dirty_domains`. Single-domain changes only rewrite that domain's data. `flush()` reads AND resets `_dirty` INSIDE the lock (Fix C3).

---

## 14. Configuration

All constants in `config.py`. Key values:

```python
LEARNING_CHAPTERS = 10          # Chapters to fetch during learning
PROFILE_MAX_AGE_DAYS = 30       # Relearn after this many days
AI_MAX_RPM = 10                 # Gemini rate limit
JS_CONTENT_RATIO = 1.5          # PW/curl length ratio → JS-heavy
JS_MIN_DIFF_CHARS = 500         # Minimum char diff → JS-heavy
MAX_CHAPTERS = 5000             # Safety cap per story
MAX_CONSECUTIVE_ERRORS = 5      # Stop after N consecutive errors
PW_MAX_CONCURRENCY = 2          # Playwright instances (CLI overridable)
```
### CLI Options
```bash
python main.py links.txt
python main.py links.txt --max-pw-instances 4
python main.py links.txt --fast-learning      # Skip optimizer
python main.py links.txt --no-validation      # Skip ProseRichnessBlock

# links.txt supports:
https://royalroad.com/fiction/...    # URL to scrape
!relearn royalroad.com               # Force re-learn domain
# comment                            # Ignored
```

---

## 15. AI Integration

### Gemini Client (`ai/client.py`)
- Token bucket rate limiter with rollback on `CancelledError` during jitter sleep
- Structured output: `response_mime_type="application/json"` + `response_schema`
- Falls back to text mode if schema causes errors

### Prompts (`ai/prompts.py`)
All prompts are static methods of `class Prompts`. Never inline f-strings in `agents.py`.

### Response Parsing (`_parse()` in `agents.py`)
```
1. Strip markdown fences
2. Fast path: try json.loads() directly
3. Slow path: raw_decode() at each '{' and '[' position (no regex, no ReDoS)
```

### `snippet()` function
- Public function (not `_snippet`) — called by utility agents and tests
- Fast path: if `len(html) <= max_len`, returns html directly (no BS4 parse)
- For large HTML: strips script/style, then get_text() as last resort

---

## 16. On the Horizon (Planned / In Progress)

### Calibration Phase (partially implemented)
Pre-scrape probe of first 10 chapters in memory with **zero-file-write mode**. AI reviews and updates site profile iteratively until zero issues across:
1. Content length
2. AI fallback usage rate
3. Title quality

Stops with detailed error report if `max_rounds` exceeded.
Planned implementation order: `config.py` → `utils/types.py` → `ai/prompts.py` → `ai/agents.py` → core modules.

### README Update (P3-20)
Deferred. Current README is a placeholder.

---

## 17. File Paths (Runtime)

```
data/site_profiles.json         # All domain profiles
data/ads_keywords.json          # Per-domain ads keywords
progress/{domain}_{slug}_{hash8}.json    # Per-story progress
output/{story_name_clean}/      # Chapter .md files
issues.md                       # Session issue log
```

---

## 18. Testing Notes

- No formal test suite currently
- `CAO_FAST_LEARNING=1` env var: skips optimizer (for testing learning flow)
- `CAO_NO_VALIDATION=1` env var: skips `ProseRichnessBlock` (read at execute time, not init)
- `PipelineRunner.default(domain)` creates a sensible default pipeline for any domain
- Blocks can be tested independently: they only need a `PipelineContext` with relevant fields set

---

## 19. Dependency Rules

```
config.py          ← no internal imports (pure constants)
utils/types.py     ← no internal imports
utils/string_helpers.py ← no internal imports
pipeline/base.py   ← no heavy deps (no BeautifulSoup)
pipeline/context.py ← imports base.py (factory only)
pipeline/executor.py ← imports all pipeline/* and core/html_filter
core/*             ← may import pipeline/*, utils/*, config
learning/*         ← may import core/*, pipeline/*, ai/*, utils/*
ai/*               ← imports config, utils/types only (not core/learning)
main.py            ← imports everything
```

**Circular import prevention**: `base.py` never imports `executor.py`. `context.py` exists as a separate factory layer to avoid this.