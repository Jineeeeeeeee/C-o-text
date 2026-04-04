"""
ai/prompts.py — Tập trung tất cả prompts gửi Gemini.

Learning Phase (10 calls, 10 chapters):
  PHASE 1 — Structure Discovery (Ch.1-4):
    AI#1 — Ch.1+2: Initial DOM structure mapping
    AI#2 — Ch.1+2: Independent cross-check (cùng data, độc lập)
    AI#3 — Ch.3+4: Selector stability validation

  PHASE 2 — Conflict Resolution (Ch.5-6):
    AI#4 — Ch.5:   Remove selectors audit (conflict detection)
    AI#5 — Ch.6:   Title extraction deep-dive + author contamination check

  PHASE 3 — Content Intelligence (Ch.7-8):
    AI#6 — Ch.7:   Special content detection (tables/math/system box)
    AI#7 — Ch.8:   Ads & watermark deep scan

  PHASE 4 — Stress Test (Ch.9-10):
    AI#8 — Ch.9:   Navigation stress test
    AI#9 — Ch.10:  Full profile simulation + quality scoring

  PHASE 5 — Synthesis:
    AI#10 — Summary: Master profile builder (nhận summary của #1-9)

Utility:
  naming_rules        — Xác định story name + chapter naming pattern
  find_first_chapter  — Tìm URL Chapter 1 từ trang Index
  classify_and_find   — Phân loại trang + tìm next URL (emergency fallback)
  verify_ads          — Xác nhận dòng text có phải ads/watermark không
"""
from __future__ import annotations


class Prompts:

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1: STRUCTURE DISCOVERY
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def learning_1_dom_structure(
        html1: str, url1: str,
        html2: str, url2: str,
    ) -> str:
        """
        AI#1 — Ch.1+2: Initial DOM structure mapping.
        Phân tích 2 chapters, liệt kê và phân loại TỪNG element quan trọng.
        """
        return f"""Bạn là chuyên gia phân tích cấu trúc HTML của web novel site.
Phân tích HTML của Chapter 1 và Chapter 2 để học cấu trúc DOM.

URL Ch.1: {url1}
HTML Ch.1 (tối đa 10000 ký tự):
{html1}

URL Ch.2: {url2}
HTML Ch.2 (tối đa 8000 ký tự):
{html2}

NHIỆM VỤ CHÍNH: Phân loại CHÍNH XÁC từng element theo vai trò thực sự.

PHÂN BIỆT RÕ RÀNG (cực kỳ quan trọng):
  • chapter_title  : Tên chương (VD: "Chapter 9 – Core Strength")
  • story_title    : Tên truyện (VD: "Rock falls, everyone dies")
  • author_name    : Tên tác giả (VD: "zechamp", "AuthorName")
  • chapter_content: Nội dung văn bản của chương
  • nav_element    : Nút/link điều hướng (Next/Prev chapter)
  • ads_element    : Quảng cáo, donation banner, watermark
  • site_chrome    : Header/footer/sidebar của website (không phải truyện)

QUY TẮC QUAN TRỌNG về remove_selectors:
  ✗ KHÔNG được add selector vào remove_selectors nếu selector đó:
    - Là ancestor (cha/ông/...) của chapter_title
    - Là ancestor của chapter_content
    - Chứa chapter_title hoặc chapter_content bên trong
  ✓ Chỉ add vào remove_selectors các element KHÔNG LIÊN QUAN đến title/content

Trả về JSON (CHỈ JSON thuần, không markdown):
{{
  "chapter_title_selector": "CSS selector chính xác nhất cho TÊN CHƯƠNG. Phải stable qua cả 2 chapters.",
  "story_title_selector": "CSS selector cho TÊN TRUYỆN (thường ở sidebar/header). null nếu không có.",
  "author_selector": "CSS selector cho TÊN TÁC GIẢ. null nếu không tìm thấy.",
  "content_selector": "CSS selector chứa TOÀN BỘ nội dung truyện. Ưu tiên #id > .class cụ thể.",
  "next_selector": "CSS selector của nút/link 'Next Chapter'. Phải là <a> có href. null nếu không có.",
  "remove_selectors": ["Selectors cần xóa. KHÔNG BAO GỜM ancestor của title/content"],
  "nav_type": "'selector' | 'rel_next' | 'slug_increment' | 'fanfic' | null",
  "chapter_url_pattern": "Regex Python nhận diện URL chapter. null nếu không đủ thông tin.",
  "requires_playwright": false,
  "title_is_inside_remove_candidate": false,
  "title_container": "CSS selector của div/section chứa title (để kiểm tra conflict). null nếu title là top-level.",
  "notes": "Ghi chú đặc biệt về cấu trúc site. null nếu không có."
}}

QUAN TRỌNG — title_is_inside_remove_candidate:
  Đặt true nếu chapter_title NẰM BÊN TRONG một element mà bình thường
  bạn muốn remove (VD: title trong div.text-center cùng với nav buttons).
  Khi true → KHÔNG được thêm container đó vào remove_selectors.
"""

    @staticmethod
    def learning_2_independent_check(
        html1: str, url1: str,
        html2: str, url2: str,
    ) -> str:
        """
        AI#2 — Ch.1+2: Independent cross-check (CÙNG DATA với AI#1, độc lập).
        Không biết kết quả AI#1. Phân tích độc lập để cross-check.
        """
        return f"""Bạn là chuyên gia phân tích HTML độc lập. Phân tích HTML của 2 chapters
và xác định selectors một cách HOÀN TOÀN ĐỘC LẬP.

URL Ch.1: {url1}
HTML Ch.1 (tối đa 10000 ký tự):
{html1}

URL Ch.2: {url2}
HTML Ch.2 (tối đa 8000 ký tự):
{html2}

NHIỆM VỤ: Xác định chính xác các CSS selectors cho từng phần.

QUY TẮC TUYỆT ĐỐI — remove_selectors:
  Trước khi thêm selector X vào remove_selectors, hãy kiểm tra:
  "Nếu tôi xóa X, tôi có vô tình xóa chapter title hoặc chapter content không?"
  Nếu CÓ hoặc KHÔNG CHẮC → KHÔNG thêm X vào remove_selectors.

PHÂN BIỆT:
  • chapter_title ≠ story_title ≠ author_name
  • Tên tác giả (author) thường nằm gần byline/profile link, KHÔNG phải h1 chính

Trả về JSON (CHỈ JSON thuần):
{{
  "chapter_title_selector": "Selector cho TÊN CHƯƠNG. null nếu không chắc.",
  "content_selector": "Selector cho NỘI DUNG TRUYỆN.",
  "next_selector": "Selector cho nút Next Chapter. null nếu không tìm thấy.",
  "remove_selectors": ["Selectors an toàn để xóa — đã verify không phải ancestor của title/content"],
  "nav_type": "'selector' | 'rel_next' | 'slug_increment' | 'fanfic' | null",
  "chapter_url_pattern": "Regex Python cho URL chapter. null nếu không đủ thông tin.",
  "author_selector": "Selector cho tên tác giả (để TRÁNH lấy làm title). null nếu không xác định.",
  "confidence": 0.85,
  "uncertain_fields": ["Liệt kê field nào bạn KHÔNG CHẮC. [] nếu tất cả chắc chắn."],
  "notes": null
}}

confidence: 0.0-1.0 — mức độ chắc chắn của bạn về TẤT CẢ selectors.
"""

    @staticmethod
    def learning_3_stability_check(
        html3: str, url3: str,
        html4: str, url4: str,
        consensus_selectors: dict,
    ) -> str:
        """
        AI#3 — Ch.3+4: Selector stability validation.
        Dùng consensus từ AI#1+#2, verify trên 2 chapters mới.
        """
        return f"""Xác nhận các CSS selectors có STABLE (ổn định) qua nhiều chapters không.

Selectors cần verify (consensus từ phân tích trước):
  content_selector      : {consensus_selectors.get('content_selector')!r}
  chapter_title_selector: {consensus_selectors.get('chapter_title_selector')!r}
  next_selector         : {consensus_selectors.get('next_selector')!r}
  remove_selectors      : {consensus_selectors.get('remove_selectors', [])}

URL Ch.3: {url3}
HTML Ch.3 (tối đa 8000 ký tự):
{html3}

URL Ch.4: {url4}
HTML Ch.4 (tối đa 8000 ký tự):
{html4}

NHIỆM VỤ:
1. Apply từng selector lên Ch.3 và Ch.4
2. Báo cáo kết quả: selector có lấy được đúng element không?
3. Nếu selector không hoạt động → đề xuất selector tốt hơn
4. Kiểm tra: có element nào trong remove_selectors chứa title/content không?

Trả về JSON (CHỈ JSON thuần):
{{
  "content_valid_ch3": true,
  "content_valid_ch4": true,
  "content_fix": null,
  "title_valid_ch3": true,
  "title_valid_ch4": true,
  "title_fix": null,
  "next_valid_ch3": true,
  "next_valid_ch4": true,
  "next_fix": null,
  "remove_selectors_safe": ["Danh sách remove_selectors đã verify an toàn"],
  "remove_selectors_dangerous": ["Selectors trong remove list CÓ THỂ xóa nhầm title/content"],
  "remove_add": ["Selector mới cần thêm vào remove list (đã verify an toàn)"],
  "stability_score": 0.95,
  "notes": null
}}

stability_score: 1.0 = tất cả selectors hoạt động đúng trên cả 4 chapters.
"""

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2: CONFLICT RESOLUTION
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def learning_4_remove_audit(
        html5: str, url5: str,
        current_remove_selectors: list[str],
        content_selector: str | None,
        title_selector: str | None,
    ) -> str:
        """
        AI#4 — Ch.5: Remove selectors audit.
        Kiểm tra từng selector trong remove list có conflict không.
        """
        remove_list = "\n".join(
            f"  {i+1}. {sel!r}"
            for i, sel in enumerate(current_remove_selectors)
        ) or "  (danh sách trống)"

        return f"""Kiểm tra xung đột (conflict) trong remove_selectors.

URL Ch.5: {url5}
content_selector : {content_selector!r}
title_selector   : {title_selector!r}

Danh sách remove_selectors cần audit:
{remove_list}

HTML Ch.5 (tối đa 8000 ký tự):
{html5}

NHIỆM VỤ: Với TỪNG selector trong remove_selectors, kiểm tra:
  Q1: Selector này có phải là TỔ TIÊN (ancestor) của content_selector không?
      Nghĩa là: nếu xóa selector này, content có bị mất không?
  Q2: Selector này có phải là TỔ TIÊN của title_selector không?
      Nghĩa là: nếu xóa selector này, chapter title có bị mất không?
  Q3: Selector này có CHỨA content hoặc title bên trong không?

Nếu Q1, Q2, hoặc Q3 = YES → selector đó là DANGEROUS, phải loại khỏi remove list.

Trả về JSON (CHỈ JSON thuần):
{{
  "audit_results": [
    {{
      "selector": ".ads-banner",
      "is_ancestor_of_content": false,
      "is_ancestor_of_title": false,
      "contains_title_or_content": false,
      "verdict": "SAFE",
      "reason": "Chỉ chứa quảng cáo, không liên quan đến title/content"
    }}
  ],
  "safe_selectors": ["Selectors đã verify SAFE để remove"],
  "dangerous_selectors": ["Selectors NGUY HIỂM — phải xóa khỏi remove list"],
  "suggested_replacements": {{
    "dangerous_selector": "selector_an_toan_hon_thay_the"
  }},
  "notes": null
}}

verdict: "SAFE" | "DANGEROUS" | "UNCERTAIN"
"""

    @staticmethod
    def learning_5_title_deepdive(
        html6: str, url6: str,
        title_selector: str | None,
        author_selector: str | None,
    ) -> str:
        """
        AI#5 — Ch.6: Title extraction deep-dive + author contamination check.
        """
        return f"""Phân tích sâu về cách lấy chapter title — phát hiện và ngăn author contamination.

URL Ch.6: {url6}
title_selector hiện tại  : {title_selector!r}
author_selector hiện tại : {author_selector!r}

HTML Ch.6 (tối đa 8000 ký tự):
{html6}

NHIỆM VỤ:
1. Tìm TẤT CẢ các cách có thể lấy chapter title:
   - Qua CSS selector (h1, h2, .chapter-title, ...)
   - Qua <title> tag
   - Qua og:title meta tag
2. Với TỪNG cách, xác định:
   - Kết quả trích xuất là gì?
   - Có lẫn tên tác giả, tên truyện, tên site không?
3. Chọn selector/source TỐT NHẤT — cho kết quả SẠCH NHẤT (chỉ chapter title)
4. Xác định tên tác giả để LOẠI TRỪ khi lấy title

PHÂN BIỆT:
  ✓ chapter_title : "Chapter 9 – Core Strength" / "Prologue: The Beginning"
  ✗ story_title   : "Rock falls, everyone dies"
  ✗ author_name   : "zechamp" / "AuthorNameFollow Author"
  ✗ site_suffix   : "| Royal Road" / "- FanFiction.net"

Trả về JSON (CHỈ JSON thuần):
{{
  "best_title_selector": "Selector cho SẠCH NHẤT chapter title only. null nếu phải dùng <title> tag.",
  "title_source_ranking": [
    {{"source": "h1.chapter-title", "sample": "Chapter 9 – Core Strength", "clean": true}},
    {{"source": "title_tag", "sample": "Chapter 9 – Core Strength - Rock falls | RR", "clean": false}}
  ],
  "author_name_detected": "zechamp",
  "author_contamination_risk": false,
  "title_cleanup_needed": false,
  "title_cleanup_note": "Nếu true: mô tả cần cleanup gì (strip suffix, split, ...)",
  "recommended_title_selector": "Selector tốt nhất sau khi xem xét tất cả",
  "notes": null
}}
"""

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3: CONTENT INTELLIGENCE
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def learning_6_special_content(html7: str, url7: str) -> str:
        """
        AI#6 — Ch.7: Special content detection.
        """
        return f"""Phân tích Chapter 7 để phát hiện nội dung đặc biệt cần formatting riêng.

URL Ch.7: {url7}
HTML (tối đa 8000 ký tự):
{html7}

Phát hiện:
  1. Bảng dữ liệu (<table>) — system status screens, stats, rankings
  2. Công thức toán học (LaTeX, MathJax, unicode)
  3. System notification boxes (RPG skill boxes, status screens)
  4. Spoiler/hidden text
  5. Author's note / Translator's note
  6. Ký hiệu đặc biệt (→, ※, ■, ●, v.v.)

Trả về JSON (CHỈ JSON thuần):
{{
  "has_tables": false,
  "table_evidence": null,
  "has_math": false,
  "math_format": null,
  "math_evidence": [],
  "system_box": {{"found": false, "selectors": [], "convert_to": "blockquote", "prefix": "**System:**"}},
  "hidden_text": {{"found": false, "selectors": [], "convert_to": "spoiler_tag"}},
  "author_note": {{"found": false, "selectors": [], "convert_to": "blockquote_note"}},
  "bold_italic": true,
  "hr_dividers": true,
  "image_alt_text": false,
  "special_symbols": [],
  "notes": null
}}

math_format: "latex" | "mathjax" | "plain_unicode" | null
"""

    @staticmethod
    def learning_7_ads_deepscan(html8: str, url8: str) -> str:
        """
        AI#7 — Ch.8: Ads & watermark deep scan.
        """
        return f"""Quét sâu để phát hiện tất cả ads, watermarks và boilerplate trong chapter.

URL Ch.8: {url8}
HTML (tối đa 8000 ký tự):
{html8}

NHIỆM VỤ:
1. Quét ĐẦU và CUỐI chapter (thường là nơi watermark xuất hiện)
2. Tìm text patterns lặp lại từ site (không phải nội dung truyện)
3. Phân biệt watermark cố định vs nội dung truyện

TIÊU CHÍ WATERMARK (✓ GIỮ vs ✗ LOẠI):
  ✓ GIỮ — Watermark cố định:
    • "Tip: You can use left, right keyboard keys..."
    • "If you find any errors, please let us know..."
    • "Read at [site]" / "Find this novel at..."
    • Boilerplate disclaimer của site
    • Navigation labels lặp lại (Prev/Next/TOC)
  ✗ LOẠI — Nội dung truyện:
    • Tên nhân vật, tên skill, plot elements
    • Dialogue nhân vật
    • Single-chapter entries

Trả về JSON (CHỈ JSON thuần):
{{
  "ads_keywords": ["watermark text lowercase — chỉ cố định, xuất hiện ≥80% chapters"],
  "ads_selectors": ["CSS selectors của ads elements trong content area — an toàn để remove"],
  "top_edge_pattern": "Text pattern xuất hiện ở đầu chapter — null nếu không có",
  "bottom_edge_pattern": "Text pattern xuất hiện ở cuối chapter — null nếu không có",
  "notes": null
}}
"""

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 4: STRESS TEST
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def learning_8_nav_stress(
        html9: str, url9: str,
        next_selector: str | None,
        nav_type: str | None,
    ) -> str:
        """
        AI#8 — Ch.9: Navigation stress test.
        """
        return f"""Kiểm tra hệ thống navigation của site trên chapter thực tế.

URL Ch.9: {url9}
next_selector hiện tại: {next_selector!r}
nav_type hiện tại     : {nav_type!r}

HTML Ch.9 (tối đa 8000 ký tự):
{html9}

NHIỆM VỤ:
1. Verify next_selector có hoạt động trên chapter này không
2. Tìm link "Next Chapter" bằng TẤT CẢ phương pháp:
   - CSS selector
   - rel="next" attribute
   - Text "Next" / "Next Chapter" / "Tiếp"
   - URL pattern increment
3. Xác nhận chapter_url_pattern có match URL này không

Trả về JSON (CHỈ JSON thuần):
{{
  "next_selector_works": true,
  "next_url_found": "URL của chapter tiếp theo nếu tìm được. null nếu không.",
  "best_next_selector": "Selector tốt nhất cho next chapter link",
  "nav_type_confirmed": "rel_next",
  "chapter_url_pattern_valid": true,
  "chapter_url_pattern_fix": null,
  "fallback_methods": ["rel_next", "anchor_text"],
  "notes": null
}}
"""

    @staticmethod
    def learning_9_full_simulation(
        html10: str, url10: str,
        profile_so_far: dict,
    ) -> str:
        """
        AI#9 — Ch.10: Full profile simulation + quality scoring.
        """
        return f"""Simulate áp dụng profile lên Chapter 10 và đánh giá chất lượng.

URL Ch.10: {url10}
Profile hiện tại:
  content_selector      : {profile_so_far.get('content_selector')!r}
  chapter_title_selector: {profile_so_far.get('chapter_title_selector')!r}
  next_selector         : {profile_so_far.get('next_selector')!r}
  remove_selectors      : {profile_so_far.get('remove_selectors', [])}
  nav_type              : {profile_so_far.get('nav_type')!r}

HTML Ch.10 (tối đa 8000 ký tự):
{html10}

NHIỆM VỤ — Simulation:
  1. Apply content_selector → trích xuất content text
  2. Apply chapter_title_selector → trích xuất title
  3. Apply next_selector → tìm next URL
  4. Apply remove_selectors → liệt kê elements bị xóa
  5. Đánh giá từng bước: đúng hay sai?

Trả về JSON (CHỈ JSON thuần):
{{
  "content_extracted": "200+ chars đầu của content đã extract (để verify). null nếu extract thất bại.",
  "content_char_count": 5420,
  "content_quality": "good",
  "title_extracted": "Chapter 10 – Something",
  "title_quality": "good",
  "next_url_found": "https://...",
  "nav_quality": "good",
  "removed_elements": ["Liệt kê elements đã bị remove — mô tả ngắn gọn"],
  "removal_safe": true,
  "overall_score": 0.95,
  "issues_found": ["Mô tả vấn đề nếu có. [] nếu không có vấn đề."],
  "field_scores": {{
    "content": 1.0,
    "title": 1.0,
    "navigation": 1.0,
    "remove_safety": 1.0
  }},
  "notes": null
}}

content_quality / title_quality / nav_quality: "good" | "partial" | "failed"
overall_score: 0.0-1.0
"""

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 5: MASTER SYNTHESIS
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def learning_10_master_synthesis(synthesis_summary: str, domain: str) -> str:
        """
        AI#10 — Master synthesis.
        Nhận summary từ 9 AI calls trước, output profile CUỐI CÙNG.
        """
        return f"""Bạn là AI tổng hợp profile cuối cùng cho web novel scraper.
Nhận kết quả từ 9 AI analysis calls trước và tổng hợp thành profile tối ưu.

Domain: {domain}

=== KẾT QUẢ TỪ 9 AI CALLS TRƯỚC ===
{synthesis_summary}
=== END SUMMARY ===

NHIỆM VỤ:
1. Tổng hợp tất cả kết quả thành profile CUỐI CÙNG
2. Khi các AI calls disagree → chọn kết quả có:
   - Confidence cao hơn
   - Được xác nhận bởi nhiều AI hơn
   - Có evidence rõ ràng hơn
3. Loại bỏ HOÀN TOÀN bất kỳ remove_selector nào từng bị đánh dấu DANGEROUS
4. Merge ads_keywords từ tất cả calls
5. Tính confidence tổng thể

QUY TẮC BẤT BIẾN (không được vi phạm):
  ✗ KHÔNG thêm selector vào remove_selectors nếu nó từng được đánh dấu dangerous
  ✗ KHÔNG để chapter_title_selector trống nếu có bất kỳ AI nào tìm được
  ✗ KHÔNG chọn author_selector làm chapter_title_selector

Trả về JSON (CHỈ JSON thuần):
{{
  "content_selector": "Selector tốt nhất đã được verify",
  "next_selector": "Selector tốt nhất hoặc null",
  "chapter_title_selector": "Selector SẠCH NHẤT cho chapter title — không lẫn author/story",
  "remove_selectors": ["Chỉ selectors đã verify SAFE — không có dangerous selector"],
  "nav_type": "rel_next",
  "chapter_url_pattern": "Regex đã verify hoặc null",
  "requires_playwright": false,
  "formatting_rules": {{
    "tables": false,
    "math_support": false,
    "math_format": null,
    "special_symbols": [],
    "bold_italic": true,
    "hr_dividers": true,
    "image_alt_text": false,
    "system_box": {{"found": false, "selectors": [], "convert_to": "blockquote", "prefix": "**System:**"}},
    "hidden_text": {{"found": false, "selectors": [], "convert_to": "spoiler_tag"}},
    "author_note": {{"found": false, "selectors": [], "convert_to": "blockquote_note"}}
  }},
  "ads_keywords": ["merged list từ tất cả calls, lowercase, chỉ watermark cố định"],
  "confidence": 0.95,
  "uncertain_fields": ["Fields nào còn uncertain sau tổng hợp"],
  "conflict_summary": "Mô tả ngắn các conflicts đã resolve và cách resolve",
  "notes": "Ghi chú về site quirks quan trọng"
}}

confidence rubric:
  0.95-1.0 : Tất cả selectors confirmed, không conflict, simulation passed
  0.80-0.94: Minor conflicts đã resolved, simulation mostly passed
  0.60-0.79: Một số uncertain fields
  < 0.60   : Nhiều conflicts chưa resolved, cần manual review
"""

    # ══════════════════════════════════════════════════════════════════════════
    # UTILITY PROMPTS (giữ nguyên)
    # ══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def naming_rules(raw_titles: list[str], base_url: str) -> str:
        numbered = "\n".join(
            f"  {i + 1}. {t!r}"
            for i, t in enumerate(raw_titles)
        )
        return f"""Phân tích các raw <title> tags của {len(raw_titles)} chapters liên tiếp để xác định cách đặt tên file.

URL ví dụ: {base_url}

Raw <title> tag content từ các chapters:
{numbered}

Nhiệm vụ:
1. Tìm TÊN TRUYỆN — phần text xuất hiện nhất quán qua tất cả chapters
2. Tìm từ khóa chapter (Chapter / Ch. / Episode / Part / ...)
3. Xác định chapters có subtitle riêng không
4. Tìm prefix cần bóc (nếu story name đứng trước "Chapter N")

Trả về JSON (CHỈ JSON thuần):
{{
  "story_name": "Tên truyện đầy đủ và chính xác.",
  "story_prefix_to_strip": "Prefix cần bóc khi tạo tên file. Chuỗi rỗng nếu không có.",
  "chapter_keyword": "Chapter | Ch. | Episode | Ep. | Part | Prologue",
  "has_chapter_subtitle": false,
  "notes": null
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
        numbered = "\n".join(
            f"  {i + 1:>2}. {line!r}"
            for i, line in enumerate(candidates)
        )
        return f"""Xác nhận dòng nào là ADS/WATERMARK thực sự.

Domain: {domain}

Các dòng cần xác nhận:
{numbered}

TIÊU CHÍ:
  ✓ Watermark cố định: "Read at [site]", donation link, copyright notice, navigation boilerplate
  ✗ Không phải ads: dialogue nhân vật, nội dung truyện, từ generic

Trả về JSON (CHỈ JSON thuần):
{{
  "confirmed_ads": ["Dòng xác nhận là ads — chép nguyên văn"],
  "false_positives": ["Dòng là false positive"],
  "notes": null
}}
"""


# ── Helper ────────────────────────────────────────────────────────────────────

def _format_profile_summary(profile: dict) -> str:
    lines = [
        f"  content_selector      : {profile.get('content_selector')!r}",
        f"  chapter_title_selector: {profile.get('chapter_title_selector')!r}",
        f"  next_selector         : {profile.get('next_selector')!r}",
        f"  remove_selectors      : {profile.get('remove_selectors', [])}",
        f"  nav_type              : {profile.get('nav_type')!r}",
        f"  has_tables            : {profile.get('formatting_rules', {}).get('tables', False)}",
        f"  has_math              : {profile.get('formatting_rules', {}).get('math_support', False)}",
        f"  system_box            : {bool(profile.get('formatting_rules', {}).get('system_box', {}).get('found'))}",
        f"  ads_keywords          : {len(profile.get('ads_keywords', []))}",
    ]
    return "\n".join(lines)