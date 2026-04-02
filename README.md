# Cào Text

Công cụ cào truyện từ web novel sites (RoyalRoad, ScribbleHub, Wattpad, fanfiction.net, ...).
Lưu từng chương thành file `.md` với formatting hoàn chỉnh — bảng, in đậm/nghiêng,
system box (LitRPG), spoiler text, author's note, công thức toán.

---

## Tính năng nổi bật

| Tính năng | Mô tả |
|---|---|
| **Thorough Learning Mode** | Lần đầu scrape một domain: 5 AI calls học toàn bộ cấu trúc site |
| **Full Scrape Mode** | Lần sau: tốc độ cao, không AI, format chuẩn từ profile đã học |
| **MarkdownFormatter** | Tables, system box, spoiler, author note, math, bold/italic, HR |
| **CF bypass** | curl_cffi (Chrome TLS fingerprint) + Playwright fallback tự động |
| **Resume an toàn** | Atomic JSON + fingerprint dedup, resume sau Ctrl+C |
| **Ads filter** | Keyword + regex lọc watermark, học từ profile |

---

## Yêu cầu

- Python 3.11+
- Gemini API key (free tier: 15 RPM — config mặc định dùng 10 RPM)

---

## Cài đặt

```bash
# 1. Vào thư mục project
cd "Cào Text"

# 2. Virtual environment (khuyến nghị)
python -m venv .venv
.venv\Scripts\activate       # Windows
source .venv/bin/activate    # Linux/macOS

# 3. Dependencies
pip install curl_cffi beautifulsoup4 google-genai python-dotenv

# 4. Playwright (chỉ cần nếu site dùng Cloudflare)
pip install playwright playwright-stealth
playwright install chromium

# 5. API key
echo "GEMINI_API_KEY=your_key_here" > .env
```

---

## Cách dùng

```bash
# Chạy với links.txt mặc định
python main.py

# Hoặc chỉ định file khác
python main.py my_links.txt
```

**`links.txt`** — mỗi dòng một URL (chapter hoặc index đều được):
```
# Dòng bắt đầu bằng # bị bỏ qua
https://www.royalroad.com/fiction/55418/the-wandering-inn
https://www.scribblehub.com/series/123456/my-novel/
https://www.fanfiction.net/s/12345678/1/My-Story
```

**Output:**
```
output/
  royalroad_com_fiction_55418/
    0001_Prologue.md
    0002_Chapter 1 - The Beginning.md
    ...
```

---

## Pipeline

```
links.txt
    │
    ▼
[Khởi động] Load profiles, khởi tạo pools
    │
    ▼ (mỗi URL chạy song song)
[Domain mới hoặc profile cũ > 30 ngày?]
    ├─ YES → Thorough Learning Mode (5 AI calls)
    │         └─ Save profile → Reset progress → Phase 3
    └─ NO  → Load profile → Phase 3
    │
    ▼
[Phase 3: Full Scrape Mode]
    ├─ Delay (lịch sự theo domain)
    ├─ Fetch HTML (curl_cffi → Playwright nếu CF)
    ├─ Clean HTML (remove hidden, remove_selectors từ profile)
    ├─ Extract content (content_selector từ profile)
    ├─ Format → Markdown (MarkdownFormatter + FormattingRules)
    ├─ Ads filter (keywords + regex từ profile)
    ├─ Fingerprint dedup
    ├─ Save .md (atomic write)
    └─ Find next URL → lặp lại
```

---

## Thorough Learning Mode (chi tiết)

Chỉ chạy **1 lần** khi domain chưa có profile (hoặc profile > 30 ngày).
Tốn ~2–3 phút, ~5 Gemini calls.

```
Ch.1 (Playwright) → AI #1: Build initial profile
                          └─ content_selector, next_selector, nav_type, ...
Ch.2 (curl_cffi)  → AI #2: Validate — selectors có hoạt động không?
Ch.3 (curl_cffi)  → AI #3: Detect tables / math / special symbols
Ch.4 (curl_cffi)  → AI #4: Detect system box / spoiler / author note
Ch.5 (curl_cffi)  → AI #5: Final cross-check + confidence score

→ Merge → SiteProfile (lưu vào data/site_profiles.json)
→ Reset progress → Scrape lại từ Ch.1 với profile hoàn chỉnh
```

### Tại sao Ch.1 dùng Playwright?
Một số site dùng JS để render content (lazy load, React, v.v.).
Playwright đảm bảo HTML đầy đủ cho AI #1 phân tích — chi phí nhỏ, chỉ 1 lần.

---

## Formatting rules (site_profiles.json)

Sau khi học, profile lưu `formatting_rules` ví dụ:

```json
{
  "domain": "www.royalroad.com",
  "confidence": 0.96,
  "content_selector": "div.chapter-content",
  "next_selector": "a.btn-primary[rel='next']",
  "formatting_rules": {
    "tables": true,
    "bold_italic": true,
    "hr_dividers": true,
    "math_support": false,
    "system_box": {
      "found": true,
      "selectors": [".well", ".panel-body"],
      "convert_to": "blockquote",
      "prefix": "**System:**"
    },
    "hidden_text": {
      "found": false
    },
    "author_note": {
      "found": true,
      "selectors": [".author-note"],
      "convert_to": "blockquote_note"
    }
  }
}
```

### Các `convert_to` được hỗ trợ

| Element | convert_to | Output |
|---|---|---|
| system_box | `blockquote` | `> **System:** ...` |
| system_box | `code_block` | ` ```\n...\n``` ` |
| hidden_text | `spoiler_tag` | `\|\|text\|\|` |
| hidden_text | `strikethrough` | `~~text~~` |
| hidden_text | `skip` | *(bỏ qua)* |
| author_note | `blockquote_note` | `> *Author's Note:* ...` |
| author_note | `italic_note` | `*[AN: ...]*` |
| author_note | `skip` | *(bỏ qua)* |

---

## Cấu hình (`config.py`)

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `MAX_CHAPTERS` | 5000 | Giới hạn chương/truyện |
| `AI_MAX_RPM` | 10 | Gemini calls/phút (free: 15) |
| `LEARNING_CHAPTERS` | 5 | Chapters dùng để học |
| `PROFILE_MAX_AGE_DAYS` | 30 | Re-learn nếu profile cũ hơn |

**Delay theo domain** (trong `_DELAY_PROFILES`):

| Domain | Delay |
|---|---|
| royalroad.com | 6–14s |
| scribblehub.com | 4–10s |
| wattpad.com | 3–8s |
| fanfiction.net | 2–6s |
| archiveofourown.org | 2–5s |
| *Khác* | 1–3s |

---

## Cấu trúc project

```
cao_text/
├── main.py                  # Entry point, AppState
├── config.py                # Hằng số, regex, delays
├── links.txt                # URLs cần cào
│
├── data/                    # Runtime data (git ignored)
│   ├── site_profiles.json   # Profiles đã học per-domain
│   └── ads_keywords.json    # Ads/watermark DB
│
├── output/                  # Chapters .md (git ignored)
├── progress/                # Progress JSON (git ignored)
│
├── ai/
│   ├── client.py            # Gemini client + rate limiter
│   ├── prompts.py           # 5 Learning + 2 Utility prompts
│   └── agents.py            # 7 agent functions
│
├── core/
│   ├── fetch.py             # CF fallback tự động
│   ├── session_pool.py      # curl_cffi + Playwright pools
│   ├── html_filter.py       # Hidden/noise/remove_selectors
│   ├── formatter.py         # MarkdownFormatter (driven by profile)
│   ├── extractor.py         # Content + title extraction
│   ├── navigator.py         # Next URL heuristics
│   └── scraper.py           # Full Scrape Mode loop
│
├── learning/
│   ├── profile_manager.py   # Thread-safe profile storage
│   └── phase.py             # Thorough Learning Mode (5 AI calls)
│
└── utils/
    ├── types.py             # TypedDicts (SiteProfile, FormattingRules, ...)
    ├── string_helpers.py    # normalize_title, make_fingerprint, ...
    ├── file_io.py           # Async-safe I/O
    └── ads_filter.py        # Keyword + regex watermark filter
```

---

## Lưu ý

- **Chỉ dùng cho mục đích cá nhân.** Kiểm tra ToS của site trước khi cào.
- **RoyalRoad / ScribbleHub** có rate limit nghiêm — không giảm delay.
- **Playwright** chỉ khởi động khi gặp Cloudflare, không tốn tài nguyên bình thường.
- **`data/site_profiles.json`** có thể commit để chia sẻ profiles giữa máy.
- **Gemini free tier**: 15 RPM. Config mặc định 10 RPM để an toàn khi chạy song song.