"""
core/formatter.py — HTML → Markdown converter được điều khiển bởi site profile.

MarkdownFormatter đọc FormattingRules từ profile và xử lý:
  ✓ Tables         → Markdown tables (nếu rules.tables = True)
  ✓ System boxes   → > **System:** ... (nếu rules.system_box có selectors)
  ✓ Hidden/spoiler → ||text|| hoặc ~~text~~ (nếu rules.hidden_text)
  ✓ Author notes   → > *Author's Note:* ... (nếu rules.author_note)
  ✓ Math           → giữ nguyên $...$ hoặc $$...$$ (nếu rules.math_support)
  ✓ Bold/italic    → **bold** / *italic* (nếu rules.bold_italic)
  ✓ HR dividers    → --- (nếu rules.hr_dividers)
  ✓ Images         → [alt text] (nếu rules.image_alt_text)
  ✓ Code           → `inline` hoặc ```block```
  ✓ Blockquotes    → > ...
  ✓ Lists          → - item / 1. item

Khi không có rules (profile chưa learn) → fallback sang plain text extraction.
"""
from __future__ import annotations

import re
from bs4 import NavigableString, Tag
from utils.types import FormattingRules, SpecialElementRule


# ── Tag categories ────────────────────────────────────────────────────────────

_SKIP_TAGS = frozenset({
    "script", "style", "noscript", "iframe", "svg",
    "canvas", "video", "audio", "source", "picture",
    "form", "input", "select", "option", "head",
    "meta", "link",
})

_BLOCK_TAGS = frozenset({
    "p", "div", "h1", "h2", "h3", "h4", "h5", "h6",
    "li", "blockquote", "pre", "article", "section",
    "tr", "td", "th", "dd", "dt", "header", "footer",
    "aside", "nav", "main",
})

_HEADING_MAP = {"h1": "#", "h2": "##", "h3": "###", "h4": "####", "h5": "#####", "h6": "######"}


class MarkdownFormatter:
    """
    Converts BeautifulSoup Tag → clean Markdown string.

    Usage:
        formatter = MarkdownFormatter(profile.get("formatting_rules"))
        markdown_text = formatter.format(content_element)
    """

    def __init__(self, rules: FormattingRules | None = None) -> None:
        self.rules: FormattingRules = rules or {}

    # ── Public API ────────────────────────────────────────────────────────────

    def format(self, element: Tag) -> str:
        """Entry point. Trả về chuỗi Markdown đã clean."""
        parts: list[str] = []
        self._visit(element, parts)
        text = "".join(parts)
        # Normalize: xóa trailing spaces, gộp blank lines thừa
        lines = [ln.rstrip() for ln in text.splitlines()]
        text  = "\n".join(lines)
        text  = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── Node visitor ──────────────────────────────────────────────────────────

    def _visit(self, node, parts: list[str]) -> None:
        # NavigableString: text node
        if isinstance(node, NavigableString):
            text = str(node)
            if text:
                parts.append(text)
            return

        if not isinstance(node, Tag) or not node.name:
            return

        tag = node.name.lower()

        # Bỏ qua hoàn toàn noise tags
        if tag in _SKIP_TAGS:
            return

        # ── Kiểm tra special element rules ───────────────────────────────────
        rules = self.rules

        sb_rule = rules.get("system_box")
        if sb_rule and sb_rule.get("found") and self._matches(node, sb_rule.get("selectors", [])):
            self._render_system_box(node, parts, sb_rule)
            return

        ht_rule = rules.get("hidden_text")
        if ht_rule and ht_rule.get("found") and self._matches(node, ht_rule.get("selectors", [])):
            self._render_hidden_text(node, parts, ht_rule)
            return

        an_rule = rules.get("author_note")
        if an_rule and an_rule.get("found") and self._matches(node, an_rule.get("selectors", [])):
            self._render_author_note(node, parts, an_rule)
            return

        # ── Standard tag rendering ────────────────────────────────────────────

        # Headings
        if tag in _HEADING_MAP:
            text = node.get_text(separator=" ", strip=True)
            if text:
                hashes = _HEADING_MAP[tag]
                parts.append(f"\n\n{hashes} {text}\n\n")
            return

        # HR
        if tag == "hr":
            if rules.get("hr_dividers", True):
                parts.append("\n\n---\n\n")
            return

        # Line break
        if tag == "br":
            parts.append("\n")
            return

        # Bold
        if tag in ("strong", "b"):
            if rules.get("bold_italic", True):
                inner = self._inner_text(node)
                stripped = inner.strip()
                if stripped:
                    # Preserve surrounding whitespace
                    lead  = inner[: len(inner) - len(inner.lstrip())]
                    trail = inner[len(inner.rstrip()):]
                    parts.append(f"{lead}**{stripped}**{trail}")
                return
            # Fallback: plain text
            self._visit_children(node, parts)
            return

        # Italic
        if tag in ("em", "i"):
            if rules.get("bold_italic", True):
                inner = self._inner_text(node)
                stripped = inner.strip()
                if stripped:
                    lead  = inner[: len(inner) - len(inner.lstrip())]
                    trail = inner[len(inner.rstrip()):]
                    parts.append(f"{lead}*{stripped}*{trail}")
                return
            self._visit_children(node, parts)
            return

        # Strikethrough
        if tag in ("del", "s", "strike"):
            inner = node.get_text(strip=True)
            if inner:
                parts.append(f"~~{inner}~~")
            return

        # Underline (không có MD equivalent, render plain)
        if tag == "u":
            self._visit_children(node, parts)
            return

        # Image
        if tag == "img":
            if rules.get("image_alt_text"):
                alt = node.get("alt", "").strip()
                if alt:
                    parts.append(f"[{alt}]")
            return

        # Table
        if tag == "table":
            if rules.get("tables", False):
                self._render_table(node, parts)
            else:
                # Fallback: extract text từ cells
                for cell in node.find_all(["td", "th"]):
                    cell_parts: list[str] = []
                    self._visit_children(cell, cell_parts)
                    text = "".join(cell_parts).strip()
                    if text:
                        parts.append(text + "\n")
            return

        # Skip table structural tags nếu đã handled bởi _render_table
        if tag in ("thead", "tbody", "tfoot", "tr", "td", "th", "colgroup", "col"):
            self._visit_children(node, parts)
            return

        # Pre / Code block
        if tag == "pre":
            code_tag = node.find("code")
            lang = ""
            if code_tag and code_tag.get("class"):
                for cls in code_tag.get("class", []):
                    if cls.startswith("language-"):
                        lang = cls[9:]
                        break
            text = (code_tag or node).get_text()
            if text.strip():
                parts.append(f"\n\n```{lang}\n{text}\n```\n\n")
            return

        # Inline code
        if tag == "code" and (not node.parent or node.parent.name != "pre"):
            text = node.get_text()
            if text.strip():
                parts.append(f"`{text}`")
            return

        # Blockquote
        if tag == "blockquote":
            inner_parts: list[str] = []
            self._visit_children(node, inner_parts)
            inner = "".join(inner_parts).strip()
            if inner:
                quoted = "\n".join(f"> {ln}" for ln in inner.splitlines())
                parts.append(f"\n\n{quoted}\n\n")
            return

        # Unordered list
        if tag == "ul":
            parts.append("\n")
            for li in node.find_all("li", recursive=False):
                li_parts: list[str] = []
                self._visit_children(li, li_parts)
                li_text = "".join(li_parts).strip()
                if li_text:
                    parts.append(f"- {li_text}\n")
            parts.append("\n")
            return

        # Ordered list
        if tag == "ol":
            parts.append("\n")
            for idx, li in enumerate(node.find_all("li", recursive=False), 1):
                li_parts = []
                self._visit_children(li, li_parts)
                li_text = "".join(li_parts).strip()
                if li_text:
                    parts.append(f"{idx}. {li_text}\n")
            parts.append("\n")
            return

        # Anchor (link): chỉ giữ text
        if tag == "a":
            self._visit_children(node, parts)
            return

        # Block-level containers: thêm newline trước/sau
        is_block = tag in _BLOCK_TAGS
        if is_block:
            parts.append("\n")

        self._visit_children(node, parts)

        if is_block:
            parts.append("\n")

    def _visit_children(self, node: Tag, parts: list[str]) -> None:
        for child in node.children:
            self._visit(child, parts)

    # ── Special element renderers ─────────────────────────────────────────────

    def _render_system_box(self, node: Tag, parts: list[str], rule: dict) -> None:
        """RPG system notification box → blockquote với prefix."""
        prefix     = rule.get("prefix", "**System:**")
        convert_to = rule.get("convert_to", "blockquote")

        inner_parts: list[str] = []
        self._visit_children(node, inner_parts)
        inner = "".join(inner_parts).strip()
        if not inner:
            return

        if convert_to == "code_block":
            parts.append(f"\n\n```\n{inner}\n```\n\n")
        else:  # blockquote (default)
            # First line has prefix, rest is quoted
            lines   = inner.splitlines()
            quoted  = "\n".join(f"> {ln}" for ln in lines)
            parts.append(f"\n\n> {prefix}\n{quoted}\n\n")

    def _render_hidden_text(self, node: Tag, parts: list[str], rule: dict) -> None:
        """Spoiler / censored text."""
        convert_to = rule.get("convert_to", "spoiler_tag")
        inner = node.get_text(separator=" ", strip=True)
        if not inner:
            return
        if convert_to == "strikethrough":
            parts.append(f"~~{inner}~~")
        elif convert_to == "skip":
            pass
        else:  # spoiler_tag (Discord/некоторые MD renderers)
            parts.append(f"||{inner}||")

    def _render_author_note(self, node: Tag, parts: list[str], rule: dict) -> None:
        """Author's note / Translator's note."""
        convert_to = rule.get("convert_to", "blockquote_note")
        inner_parts: list[str] = []
        self._visit_children(node, inner_parts)
        inner = "".join(inner_parts).strip()
        if not inner:
            return
        if convert_to == "italic_note":
            parts.append(f"\n\n*[Author's Note: {inner}]*\n\n")
        elif convert_to == "skip":
            pass
        else:  # blockquote_note
            lines  = inner.splitlines()
            quoted = "\n".join(f"> {ln}" for ln in lines)
            parts.append(f"\n\n> *Author's Note:*\n{quoted}\n\n")

    def _render_table(self, table: Tag, parts: list[str]) -> None:
        """HTML table → Markdown table."""
        rows: list[list[str]] = []

        for tr in table.find_all("tr"):
            cells: list[str] = []
            for cell in tr.find_all(["td", "th"]):
                cell_parts: list[str] = []
                self._visit_children(cell, cell_parts)
                cell_text = "".join(cell_parts).strip().replace("|", "\\|")
                # Flatten newlines trong cell
                cell_text = " ".join(cell_text.split())
                cells.append(cell_text)
            if cells:
                rows.append(cells)

        if not rows:
            return

        # Normalize column count
        max_cols = max(len(r) for r in rows)
        for row in rows:
            while len(row) < max_cols:
                row.append("")

        parts.append("\n\n")
        parts.append("| " + " | ".join(rows[0]) + " |\n")
        parts.append("| " + " | ".join(["---"] * max_cols) + " |\n")
        for row in rows[1:]:
            parts.append("| " + " | ".join(row) + " |\n")
        parts.append("\n")

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _inner_text(self, node: Tag) -> str:
        """Get formatted text của node (recursive nhưng không wrap ngoài)."""
        parts: list[str] = []
        self._visit_children(node, parts)
        return "".join(parts)

    def _matches(self, el: Tag, selectors: list[str]) -> bool:
        """
        Kiểm tra element có khớp với bất kỳ selector nào không.
        Hỗ trợ: .class, #id, tag, tag.class, tag#id, .class1.class2
        """
        if not selectors:
            return False
        el_classes = set(el.get("class") or [])
        el_id      = el.get("id", "")
        el_tag     = el.name or ""

        for sel in selectors:
            sel = sel.strip()
            if not sel:
                continue
            try:
                # Compound: tag.class hoặc tag#id
                if re.match(r"^[a-zA-Z][\w]*[.#]", sel):
                    # Try BeautifulSoup select on parent
                    if el.parent:
                        matched = el.parent.select(sel)
                        if el in matched:
                            return True
                    continue

                # .class (single) hoặc .class1.class2 (compound)
                if sel.startswith("."):
                    # Tách tất cả class names: ".foo.bar" → ["foo", "bar"]
                    classes = [p for p in sel.split(".") if p]
                    if classes and all(c in el_classes for c in classes):
                        return True
                    continue

                # #id
                if sel.startswith("#"):
                    if el_id == sel[1:]:
                        return True
                    continue

                # tag
                if sel == el_tag:
                    return True

            except Exception:
                pass
        return False


# ── Fallback plain text extractor (khi chưa có profile) ──────────────────────

_PLAIN_SKIP = frozenset({
    "script", "style", "noscript", "iframe", "svg",
    "canvas", "button", "select", "option", "form",
    "figure", "picture", "source", "video", "audio",
})
_PLAIN_BLOCK = frozenset({
    "p", "div", "h1", "h2", "h3", "h4", "h5", "h6",
    "li", "blockquote", "pre", "article", "section",
    "tr", "td", "th",
})


def extract_plain_text(element: Tag) -> str:
    """
    Fallback khi không có profile: extract plain text với block-aware newlines.
    Dùng khi chưa chạy Learning Phase.
    """
    parts: list[str] = []

    def _recurse(node) -> None:
        if isinstance(node, NavigableString):
            parts.append(str(node))
            return
        if not isinstance(node, Tag) or not node.name:
            return
        tag = node.name.lower()
        if tag in _PLAIN_SKIP:
            return
        if tag == "br":
            parts.append("\n")
            return
        is_block = tag in _PLAIN_BLOCK
        if is_block:
            parts.append("\n")
        for child in node.children:
            _recurse(child)
        if is_block:
            parts.append("\n")

    _recurse(element)
    text  = "".join(parts)
    lines = [ln.rstrip() for ln in text.splitlines()]
    text  = "\n".join(lines)
    text  = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()