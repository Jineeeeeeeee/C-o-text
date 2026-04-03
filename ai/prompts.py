"""
ai/prompts.py — Tập trung tất cả prompts gửi Gemini.

Learning Phase (5 calls):
  1. build_initial_profile   — Học selectors cơ bản từ Chapter 1
  2. validate_selectors      — Xác nhận selectors từ Chapter 2
  3. analyze_special_content — Bảng/toán/ký hiệu từ Chapter 3
  4. analyze_formatting      — System box/spoiler/author note từ Chapter 4
  5. final_crosscheck        — Tổng hợp & confidence score từ Chapter 5

Utility:
  find_first_chapter  — Tìm URL Chapter 1 từ trang Index
  classify_and_find   — Phân loại trang + tìm next URL (emergency fallback)
  verify_ads          — Xác nhận dòng text có phải ads/watermark không
"""
from __future__ import annotations


class Prompts:

    @staticmethod
    def learning_5_final_crosscheck(html_snippet: str, url: str, accumulated_profile: dict) -> str:
        """
        FIX: Cải thiện tiêu chí phân biệt watermark cố định vs nội dung truyện biến động.
        Tránh học keyword quá generic hoặc nội dung story.
        """
        return f"""Bạn đã phân tích 4 chương trước. Đây là Chapter 5 — hãy cross-check và finalize profile.

URL Chapter 5: {url}
HTML (tối đa 8000 ký tự):
{html_snippet}

Profile hiện tại (tích lũy từ 4 chapter trước):
{_format_profile_summary(accumulated_profile)}

Nhiệm vụ:
1. Xác nhận content_selector/next_selector/title_selector có hoạt động trên Chapter 5 không
2. Nếu cần fix → đưa ra selector final tốt nhất
3. **QUAN TRỌNG**: Scan Chapter 5 tìm **CHỈ watermark/ads cố định** (lặp lại ở HẦUHẾT chapters)
4. Đánh giá confidence tổng thể (0.0–1.0)

Trả về JSON (CHỈ JSON thuần):
{{
  "content_selector_final": "Selector tốt nhất — giữ nguyên hoặc cải thiện. null chỉ khi không tìm được.",
  "next_selector_final": "Selector tốt nhất hoặc null.",
  "title_selector_final": "Selector tốt nhất hoặc null.",
  "remove_selectors_final": ["Danh sách ĐẦYĐỦ các selectors cần remove (tích hợp tất cả từ 5 chương)"],
  "ads_keywords": ["Chỉ watermark/ads CỐ ĐỊNH xuất hiện ≥80% chapters, lowercase. Tối đa 10."],
  "confidence": 0.95,
  "notes": "Tóm tắt ngắn về profile chất lượng và bất kỳ quirk nào của site."
}}

TIÊU CHÍ ADS KEYWORDS (✓ GIỮ vs ✗ LOẠI):

✓ GIỮ LẠI - Watermark cố định (lặp lại hầu hết chapters):
  • "Tip: You can use left, right keyboard keys..." (lặp 100/74 chapters)
  • "If you find any errors, please let us know..." (lặp 74/74 chapters)
  • "Read at [site]" / "Visit [site]" / "Find this novel at..." (lặp nhiều chapters)
  • <script type="text/javascript">window.pubfuturetag...</script> (lặp 40+ chapters)
  • Boilerplate disclaimer của site (đặc trưng, lặp lại)

✗ LOẠI BỎ - Nội dung truyện hoặc từ generic:
  • "Searching for" / "searching" (chỉ vài chapters, là nội dung story)
  • "the primal hunter" / "bloodline of the primal hunter" (tên story/skill, nội dung)
  • "search" / "log in" / "read" / "find" (quá generic, match cả nội dung truyện)
  • "royal road" (tên site generic, match dialogue nhân vật)
  • Tên nhân vật, tên skill, hoặc plot elements (biến động, không cố định)
  • Dialogue hoặc narrative của story (part của content, không watermark)
  • Single-chapter/rare entries (chỉ xuất hiện 1-3 chapters)

confidence rubric:
  0.95–1.0: Tất cả selectors confirmed, nav tốt, content clean, ads rõ ràng cố định
  0.80–0.94: Selectors hoạt động nhưng có minor issues
  0.60–0.79: Có 1-2 vấn đề chưa giải quyết được
  < 0.60: Nhiều vấn đề, cần manual review
"""

    @staticmethod
    def learning_1_initial_profile(html_snippet: str, url: str) -> str:
        return f"""Bạn là chuyên gia phân tích cấu trúc web novel site.
Phân tích HTML của Chapter 1 và trích xuất thông tin cấu trúc site.

URL: {url}
HTML (tối đa 10000 ký tự):
{html_snippet}

Trả về JSON (CHỈ JSON thuần, không markdown fence, không comment):
{{
  "content_selector": "CSS selector chứa TOÀN BỘ nội dung truyện (văn bản chương). Ưu tiên #id > .class cụ thể > tag[attr]. KHÔNG ĐƯỢC chọn body, html, main, hay element chứa sidebar/nav.",
  "next_selector": "CSS selector của nút/link 'Next Chapter'. Phải là <a> hoặc <button> có href. null nếu không tìm thấy.",
  "title_selector": "CSS selector của tiêu đề CHƯƠNG (không phải tên truyện). Thường là h1, h2, hoặc div.chapter-title. null nếu không rõ.",
  "remove_selectors": ["CSS selectors của element cần XÓA trước khi extract: ads, donation banner, chapter nav ở đầu/cuối, social share buttons. Mảng rỗng [] nếu không có."],
  "nav_type": "Cách tìm chapter tiếp theo: 'selector' (dùng next_selector), 'rel_next' (có <link rel=next>), 'slug_increment' (URL có số tăng dần), 'fanfic' (fanfiction.net /s/id/num/). null nếu không rõ.",
  "chapter_url_pattern": "Regex Python nhận diện URL chapter của site này. VD royalroad: '/fiction/\\\\d+/[^/]+/chapter/\\\\d+'. null nếu không đủ thông tin.",
  "requires_playwright": false,
  "notes": "Ghi chú đặc biệt về site (JS-heavy, paywall, CDN, v.v.). null nếu không có."
}}

Quy tắc bắt buộc:
- content_selector: test lại bằng cách đọc HTML — selector PHẢI match element chứa văn bản truyện thực sự
- Nếu content div chứa nút Prev/Next bên trong → thêm các selector đó vào remove_selectors
- requires_playwright: chỉ true nếu thấy bằng chứng site cần JS để render content (VD: div rỗng với data-src)
- Trả null cho bất kỳ field nào không đủ bằng chứng, KHÔNG suy đoán bừa
"""

    @staticmethod
    def learning_2_validate(html_snippet: str, url: str, current_selectors: dict) -> str:
        return f"""Xác nhận CSS selectors đã học từ Chapter 1 có hoạt động đúng trên Chapter 2 không.

URL Chapter 2: {url}
Selectors cần xác nhận:
  content_selector: {current_selectors.get('content_selector')!r}
  next_selector:    {current_selectors.get('next_selector')!r}
  title_selector:   {current_selectors.get('title_selector')!r}
  remove_selectors: {current_selectors.get('remove_selectors', [])}

HTML Chapter 2 (tối đa 8000 ký tự):
{html_snippet}

Kiểm tra từng selector:
1. content_selector có match element chứa ≥300 ký tự nội dung truyện không?
2. next_selector có match link/nút dẫn sang Chapter 3 không?
3. title_selector có match tiêu đề chương không?

Trả về JSON (CHỈ JSON thuần):
{{
  "content_valid": true,
  "content_fix": null,
  "next_valid": true,
  "next_fix": null,
  "title_valid": true,
  "title_fix": null,
  "remove_add": ["Thêm selector mới vào remove_selectors nếu thấy noise mới"],
  "notes": "Nhận xét ngắn. null nếu không có."
}}

Quy tắc:
- *_fix: chỉ điền nếu *_valid = false. Đưa ra selector TỐT HƠN dựa trên HTML Chapter 2.
- remove_add: chỉ thêm nếu thấy element rõ ràng là noise/ads KHÔNG có trong Chapter 1
- Nếu tất cả đều valid → tất cả trả true, các fix = null
"""

    @staticmethod
    def learning_3_special_content(html_snippet: str, url: str) -> str:
        return f"""Phân tích Chapter 3 để phát hiện nội dung đặc biệt: bảng, công thức toán, ký hiệu đặc biệt.

URL Chapter 3: {url}
HTML (tối đa 8000 ký tự):
{html_snippet}

Trả về JSON (CHỈ JSON thuần):
{{
  "has_tables": false,
  "table_evidence": "Mô tả ngắn về bảng nếu có (VD: 'status table với stat numbers'). null nếu không có.",
  "has_math": false,
  "math_format": null,
  "math_evidence": ["Ví dụ công thức thực tế tìm thấy trong HTML, tối đa 3"],
  "special_symbols": ["Ký hiệu đặc biệt quan sát được ngoài ASCII thông thường: —, …, ™, ©, ·, →, ⟨⟩, v.v."],
  "notes": "Ghi chú đặc biệt về nội dung. null nếu không có."
}}

Hướng dẫn math_format:
  "latex"         — nội dung dùng $...$ hoặc $$...$$ (inline/block LaTeX)
  "mathjax"       — nội dung dùng \\(...\\) hoặc \\[...\\] hoặc có class MathJax
  "plain_unicode" — công thức viết bằng ký tự unicode thường (x², √, ∑, v.v.)
  null            — không có công thức toán
"""

    @staticmethod
    def learning_4_formatting(html_snippet: str, url: str) -> str:
        return f"""Phân tích Chapter 4 để phát hiện các element định dạng đặc biệt:
system notification box, hidden/spoiler text, author's note / translator's note.

URL Chapter 4: {url}
HTML (tối đa 8000 ký tự):
{html_snippet}

Trả về JSON (CHỈ JSON thuần):
{{
  "system_box": {{
    "found": false,
    "selectors": ["CSS selectors của system/notification box. VD: ['.well', 'div.system', '.panel-body']"],
    "convert_to": "blockquote",
    "prefix": "**System:**"
  }},
  "hidden_text": {{
    "found": false,
    "selectors": ["CSS selectors của spoiler/hidden text. VD: ['.spoiler', '.hidden', 'span[style*=color:white]']"],
    "convert_to": "spoiler_tag"
  }},
  "author_note": {{
    "found": false,
    "selectors": ["CSS selectors của author note / TN. VD: ['.author-note', '.translator-note', '.an']"],
    "convert_to": "blockquote_note"
  }},
  "bold_italic": true,
  "hr_dividers": true,
  "image_alt_text": false,
  "notes": "Ghi chú về formatting đặc biệt khác. null nếu không có."
}}
"""

    @staticmethod
    def find_first_chapter(candidates: str, base_url: str) -> str:
        return f"""Đây là các URL candidate cho Chapter 1 của truyện:
{candidates}

Trang nguồn: {base_url}

Trả về JSON (CHỈ JSON thuần):
{{"first_chapter_url": "URL của Chapter 1 — chương đầu tiên, số nhỏ nhất. null nếu không xác định được."}}
"""

    @staticmethod
    def classify_and_find(hint_block: str, html_snippet: str, base_url: str) -> str:
        return f"""Phân loại trang và tìm URL chương tiếp theo (emergency fallback).

URL hiện tại: {base_url}
Link điều hướng:
{hint_block}

HTML (tối đa 5000 ký tự):
{html_snippet}

Trả về JSON (CHỈ JSON thuần):
{{
  "page_type": "chapter",
  "next_url": "URL chương tiếp theo hoặc null",
  "first_chapter_url": null
}}
"""

    @staticmethod
    def verify_ads(candidates: list[str], domain: str) -> str:
        """
        Xác nhận danh sách dòng text có phải ads/watermark thật không.
        Gọi sau mỗi phiên scrape để validate những gì đã bị lọc.
        """
        numbered = "\n".join(
            f"  {i + 1:>2}. {line!r}"
            for i, line in enumerate(candidates)
        )
        return f"""Bạn là chuyên gia lọc nội dung web novel. Nhiệm vụ: xác nhận dòng nào là ADS/WATERMARK thực sự.

Domain scrape: {domain}

Các dòng đã bị lọc ra khỏi nội dung truyện (xuất hiện nhiều lần):
{numbered}

TIÊU CHÍ ADS/WATERMARK (xác nhận là TRUE):
  ✓ Thông báo stolen content, piracy notice
  ✓ "Read at [site]", "Visit [site]", "Find this novel at..."
  ✓ Quảng cáo Patreon / Ko-fi / donation kêu gọi donate
  ✓ Attribution dịch thuật lặp đi lặp lại dạng boilerplate (không phải dialogue nhân vật)
  ✓ Navigation label lặp lại (Prev Chapter / Next Chapter / Table of Contents)
  ✓ Copyright notice/watermark chèn vào content

KHÔNG PHẢI ADS (xác nhận là FALSE — false positive):
  ✗ Dialogue nhân vật tình cờ đề cập tên website
  ✗ Nội dung truyện đề cập dịch thuật/ngôn ngữ trong context câu chuyện
  ✗ Mô tả sách/tài liệu trong fictional world
  ✗ Bất kỳ câu nào rõ ràng là văn học hư cấu
  ✗ Từ quá generic ("search", "log in", "read") nếu match cả nội dung truyện

Trả về JSON (CHỈ JSON thuần, không markdown fence):
{{
  "confirmed_ads": [
    "Chép NGUYÊN VĂN những dòng xác nhận là ads thật. Mảng rỗng [] nếu không có."
  ],
  "false_positives": [
    "Chép NGUYÊN VĂN những dòng là false positive (không phải ads). Mảng rỗng [] nếu không có."
  ],
  "notes": "Ghi chú ngắn nếu có pattern đặc biệt. null nếu không cần."
}}
"""


# ── Helper ────────────────────────────────────────────────────────────────────

def _format_profile_summary(profile: dict) -> str:
    lines = [
        f"  content_selector:  {profile.get('content_selector')!r}",
        f"  next_selector:     {profile.get('next_selector')!r}",
        f"  title_selector:    {profile.get('title_selector')!r}",
        f"  remove_selectors:  {profile.get('remove_selectors', [])}",
        f"  nav_type:          {profile.get('nav_type')!r}",
        f"  has_tables:        {profile.get('formatting_rules', {}).get('tables', False)}",
        f"  has_math:          {profile.get('formatting_rules', {}).get('math_support', False)}",
        f"  system_box:        {bool(profile.get('formatting_rules', {}).get('system_box', {}).get('found'))}",
        f"  author_note:       {bool(profile.get('formatting_rules', {}).get('author_note', {}).get('found'))}",
    ]
    return "\n".join(lines)