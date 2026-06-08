# pdf.py — Stage 3a: PDF Parser (PDFLineExtractor)
#
#   Extracts every line with font metadata (size, bold, italic) and vertical
#   gaps using pdfplumber's extract_words().  Heading detection uses the rich
#   per-line metadata to identify H1/H2 headings, then produces a standard
#   ParsedDocument for downstream chunking.

"""Stage 3a — PDF parser using pdfplumber with word-level line extraction."""

import io
from collections import Counter
from dataclasses import dataclass

import pdfplumber

from app.services.data_ingestion.pipeline.parsers.data_class.document import ParsedDocument, HeadingNode
from app.services.data_ingestion.pipeline.parsers.base import BaseParser

# Lines starting with these are unlikely to be headings
_BULLET_PREFIXES = ("▪", "•", "-", "–", "—", "*", "●")
_SKIP_PREFIXES = ("http://", "https://", "www.")


@dataclass
class RawLine:
    text: str
    page: int
    y_pos: float
    avg_font_size: float
    is_bold: bool
    is_italic: bool
    line_index: int
    gap_before: float


class PDFLineExtractor(BaseParser):
    """Extract every line with font metadata, detect headings, produce ParsedDocument."""

    def parse(self, data: bytes, filename: str, mime_type: str) -> ParsedDocument:
        lines, doc_meta = self._extract(data)
        headings = self._detect_headings(lines)
        raw_text = self._build_raw_text(lines)
        headings = self._filter_bullet_headings(headings, raw_text)
        title = self._extract_title(lines)

        return ParsedDocument(
            source_filename=filename,
            mime_type=mime_type,
            title=title,
            raw_text=raw_text,
            headings=headings,
            page_count=doc_meta.get("total_pages"),
            raw_lines=lines,
            metadata=doc_meta,
        )

    def _extract(self, data: bytes) -> tuple[list[RawLine], dict]:
        """Extract lines with font metadata from PDF bytes."""
        lines: list[RawLine] = []
        global_idx = 0
        prev_bottom = None
        prev_page = None

        with pdfplumber.open(io.BytesIO(data)) as pdf:
            doc_meta = {
                "total_pages": len(pdf.pages),
                "pdf_metadata": pdf.metadata or {},
            }
            for page_num, page in enumerate(pdf.pages, 1):
                words = page.extract_words(
                    extra_attrs=["fontname", "size"],
                    keep_blank_chars=True,
                    use_text_flow=True,
                )
                if not words:
                    continue
                grouped = self._group_into_lines(words)
                for line_words in grouped:
                    text = " ".join(w["text"] for w in line_words).strip()
                    if not text:
                        continue
                    avg_size = sum(w.get("size", 11) for w in line_words) / len(line_words)
                    is_bold = any("bold" in str(w.get("fontname", "")).lower() for w in line_words)
                    is_italic = any(
                        "italic" in str(w.get("fontname", "")).lower()
                        or "oblique" in str(w.get("fontname", "")).lower()
                        for w in line_words
                    )
                    y_top = min(w.get("top", 0) for w in line_words)

                    gap = 0.0
                    if prev_bottom is not None and page_num == prev_page:
                        gap = max(0, y_top - prev_bottom)

                    lines.append(RawLine(
                        text=text, page=page_num, y_pos=round(y_top, 1),
                        avg_font_size=round(avg_size, 1),
                        is_bold=is_bold, is_italic=is_italic,
                        line_index=global_idx, gap_before=round(gap, 1),
                    ))
                    prev_bottom = max(w.get("bottom", 0) for w in line_words)
                    prev_page = page_num
                    global_idx += 1
        return lines, doc_meta

    def _group_into_lines(self, words: list[dict]) -> list[list[dict]]:
        if not words:
            return []
        lines: list[list[dict]] = []
        current = [words[0]]
        current_top = words[0].get("top", 0)
        for w in words[1:]:
            if abs(w.get("top", 0) - current_top) <= 3:
                current.append(w)
            else:
                lines.append(current)
                current = [w]
                current_top = w.get("top", 0)
        lines.append(current)
        return lines

    def _detect_headings(self, lines: list[RawLine]) -> list[HeadingNode]:
        """Identify headings using font-size, bold, and ALL-CAPS heuristics."""
        if not lines:
            return []

        # Determine body font size (most common)
        size_counts: Counter[float] = Counter()
        for line in lines:
            size_counts[line.avg_font_size] += 1
        body_size = size_counts.most_common(1)[0][0]

        headings: list[HeadingNode] = []
        seen: set[str] = set()

        for line in lines:
            text = line.text
            is_bullet = any(text.startswith(p) for p in _BULLET_PREFIXES)
            is_skip = any(text.lower().startswith(p) for p in _SKIP_PREFIXES)

            if is_skip or is_bullet or len(text) < 3:
                continue

            heading_level: int | None = None

            # Heuristic 1: Font size notably larger than body -> H1
            if line.avg_font_size > body_size + 1.0 and len(text) < 120:
                heading_level = 1
            # Heuristic 2: Bold standalone short line -> H2
            elif line.is_bold and len(text) < 50:
                heading_level = 2
            # Heuristic 3: ALL CAPS fallback -> H1
            elif text.isupper() and len(text) < 120:
                heading_level = 1

            if heading_level and text not in seen:
                seen.add(text)
                headings.append(HeadingNode(level=heading_level, title=text))

        return headings

    @staticmethod
    def _build_raw_text(lines: list[RawLine]) -> str:
        """Join line texts, inserting double newlines for large vertical gaps."""
        if not lines:
            return ""
        parts: list[str] = []
        for line in lines:
            if line.gap_before > 10:
                parts.append("\n" + line.text)
            else:
                parts.append(line.text)
        return "\n".join(parts)

    @staticmethod
    def _filter_bullet_headings(
        headings: list[HeadingNode], raw_text: str
    ) -> list[HeadingNode]:
        """Remove headings whose title appears as a bullet item in raw text."""
        filtered: list[HeadingNode] = []
        search_from = 0
        for h in headings:
            idx = raw_text.find(h.title, search_from)
            if idx == -1:
                filtered.append(h)
                continue
            line_start = raw_text.rfind("\n", 0, idx)
            line_start = line_start + 1 if line_start != -1 else 0
            prefix = raw_text[line_start:idx].strip()
            if prefix and any(prefix.startswith(p) for p in _BULLET_PREFIXES):
                continue
            filtered.append(h)
            search_from = idx + len(h.title)
        return filtered

    @staticmethod
    def _extract_title(lines: list[RawLine]) -> str | None:
        if lines:
            return lines[0].text[:256]
        return None
