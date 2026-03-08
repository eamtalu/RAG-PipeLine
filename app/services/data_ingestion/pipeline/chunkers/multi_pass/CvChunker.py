"""
Two-Pass Contextual PDF Chunker
================================

Generic engine that works on ANY document type, with optional profile
overrides for higher precision on known formats.

Architecture:
  Pass 0:   Font Cluster Analysis — maps font sizes to heading levels
  Pass 1:   Score every line — boundary map (generic + profile signals)
  Pass 2:   Carve → Nest → Walk → Emit chunks (POMA-style context paths)
"""

import re
from collections import Counter
from dataclasses import dataclass, field

from app.services.data_ingestion.pipeline.parsers.pdf import RawLine
from app.services.data_ingestion.pipeline.chunkers.multi_pass.Base import BaseChunker, ChunkResult


# ══════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

@dataclass
class ChunkerConfig:
    max_chunk_tokens: int = 512
    overlap_tokens: int = 64
    min_chunk_tokens: int = 40
    chars_per_token: float = 4.0


# ══════════════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════════════

@dataclass
class FontCluster:
    size_range: tuple
    representative: float
    is_body: bool
    heading_level: int       # 0 = body, 1 = title, 2 = h1, ...
    bold_variant_level: int


@dataclass
class Boundary:
    line_index: int
    text: str
    level: int
    page: int
    confidence: float
    signals: list


@dataclass
class Section:
    heading: str
    level: int
    content_lines: list
    page_start: int
    page_end: int
    children: list = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════
#  PASS 0: FONT CLUSTER ANALYSIS
# ══════════════════════════════════════════════════════════════════════

class FontClusterAnalyzer:

    def analyze(self, lines: list[RawLine]) -> dict[float, FontCluster]:
        if not lines:
            return {}

        size_counter: Counter = Counter()
        for line in lines:
            rounded = round(line.avg_font_size * 2) / 2
            size_counter[rounded] += len(line.text.split())

        if not size_counter:
            return {}

        body_size = size_counter.most_common(1)[0][0]
        all_sizes = sorted(size_counter.keys(), reverse=True)
        sizes_above = [s for s in all_sizes if s > body_size]
        sizes_at = [s for s in all_sizes if s == body_size]
        sizes_below = [s for s in all_sizes if s < body_size]

        clusters: dict[float, FontCluster] = {}

        for rank, size in enumerate(sizes_above):
            level = min(rank + 1, 6)
            clusters[size] = FontCluster(
                size_range=(size - 0.25, size + 0.25),
                representative=size, is_body=False,
                heading_level=level, bold_variant_level=level,
            )

        bold_level = min(len(sizes_above) + 1, 6)
        for size in sizes_at:
            clusters[size] = FontCluster(
                size_range=(size - 0.25, size + 0.25),
                representative=size, is_body=True,
                heading_level=0, bold_variant_level=bold_level,
            )

        for size in sizes_below:
            clusters[size] = FontCluster(
                size_range=(size - 0.25, size + 0.25),
                representative=size, is_body=False,
                heading_level=0, bold_variant_level=0,
            )

        return clusters

    def get_cluster(self, font_size: float, clusters: dict) -> FontCluster | None:
        rounded = round(font_size * 2) / 2
        if rounded in clusters:
            return clusters[rounded]
        best = None
        best_dist = float("inf")
        for size, cluster in clusters.items():
            dist = abs(font_size - size)
            if dist < best_dist:
                best_dist = dist
                best = cluster
        return best


# ══════════════════════════════════════════════════════════════════════
#  PROFILES — Document-type-specific signal boosters
# ══════════════════════════════════════════════════════════════════════

class BaseProfile:
    """Universal structural patterns that work on any document."""
    name = "generic"

    HEADING_PATTERNS = [
        (r"^\d+\.\s+[A-Z]", "numbered_h1"),
        (r"^\d+\.\d+\.?\s+", "numbered_h2"),
        (r"^\d+\.\d+\.\d+\.?\s+", "numbered_h3"),
        (r"^[A-Z]\.\s+[A-Z]", "letter_heading"),
        (r"^(?:CHAPTER|PART)\s+\w+", "chapter_part"),
    ]

    BULLET_RE = re.compile(r'^[▪•●○◆◇■□–—\-\*]\s')
    DASH_LIST_RE = re.compile(r'^-\s+')

    def __init__(self):
        self._heading_pats = [(re.compile(p, re.IGNORECASE), tag) for p, tag in self.HEADING_PATTERNS]

    def score_line(self, line: RawLine, all_lines: list, idx: int,
                   cluster: FontCluster | None, font_clusters: dict,
                   median_gap: float) -> tuple[float, list[str]]:
        score = 0.0
        signals: list[str] = []
        text = line.text.strip()
        wc = len(text.split())

        if cluster and cluster.heading_level > 0:
            if wc <= 10:
                bump = 0.50 - (cluster.heading_level - 1) * 0.05
                score += max(bump, 0.20)
                signals.append(f"font_level_{cluster.heading_level}")
            elif wc <= 15 and not text.endswith('.'):
                bump = 0.25 - (cluster.heading_level - 1) * 0.05
                score += max(bump, 0.10)
                signals.append(f"font_level_{cluster.heading_level}_weak")

        if (cluster and cluster.is_body and line.is_bold
                and wc <= 12 and not self._is_bullet(text)):
            score += 0.30
            signals.append("bold_body_short")

        for pat, tag in self._heading_pats:
            if pat.match(text):
                score += 0.45
                signals.append(tag)
                break

        if re.match(r'^[A-Z].*:', text) and wc <= 8:
            score += 0.35
            signals.append("ends_colon")

        alpha = re.sub(r'[^a-zA-Z\s]', '', text)
        if (alpha.isupper() and len(alpha) > 3 and 2 <= wc <= 10
                and not self._is_bullet(text)):
            score += 0.30
            signals.append("all_caps")

        if median_gap > 0 and line.gap_before > median_gap * 2.5 and wc <= 15:
            score += 0.15
            signals.append("large_gap")

        if self._is_bullet(text):
            score -= 0.40
            signals.append("pen_bullet")
        if wc > 15:
            score -= 0.30
            signals.append("pen_long")
        elif wc > 10:
            score -= 0.10
            signals.append("pen_medium")
        if text and text[0].islower():
            score -= 0.20
            signals.append("pen_lowercase")
        if text.endswith('.') and wc > 5:
            score -= 0.10
            signals.append("pen_sentence")

        return score, signals

    def assign_level(self, line: RawLine, signals: list[str],
                     cluster: FontCluster | None) -> int:
        if cluster and cluster.heading_level > 0:
            font_sigs = [s for s in signals if s.startswith("font_level_") and "_weak" not in s]
            if font_sigs:
                return cluster.heading_level

        if "bold_body_short" in signals and cluster:
            return cluster.bold_variant_level

        if "numbered_h1" in signals or "letter_heading" in signals:
            return 2
        if "numbered_h2" in signals:
            return 3
        if "numbered_h3" in signals:
            return 4
        if "chapter_part" in signals:
            return 1
        if "all_caps" in signals:
            return 2
        if "ends_colon" in signals:
            return 3

        return 3

    def adjust_levels(self, boundaries: list[Boundary]) -> list[Boundary]:
        return boundaries

    def _is_bullet(self, text: str) -> bool:
        return bool(self.BULLET_RE.match(text) or self.DASH_LIST_RE.match(text))


class CVProfile(BaseProfile):
    name = "cv"

    TOP_SECTIONS = [
        r"^(summary|profile|objective|about\s*me)\s*:?\s*$",
        r"^(core\s+)?(technical\s+)?skills\s*:?\s*$",
        r"^(work|professional|employment)\s*(experience|history)\s*:?\s*$",
        r"^(education|qualifications?|academic)\s*(background)?\s*:?\s*$",
        r"^(certifications?|awards?|honors?|achievements?)\s*(and|&)?\s*(awards?|certifications?)?\s*:?\s*$",
        r"^(community|memberships?|activities|volunteer)\s*(membership|involvement)?\s*:?\s*$",
        r"^(open[\s-]?source|oss)\s*(contributions?|projects?)?\s*:?\s*$",
        r"^(references?|referees?)\s*:?\s*$",
        r"^(publications?|papers?|research)\s*:?\s*$",
        r"^(languages?|interests?|hobbies)\s*:?\s*$",
    ]
    SUB_SECTIONS = [
        r"^(major\s+)?projects?\s*:?\s*$",
        r"^(key|notable|selected)\s+(projects?|achievements?)\s*:?\s*$",
        r"^recent\s+side\s+projects?\s*:?\s*$",
        r"^(responsibilities|duties|key\s+responsibilities)\s*:?\s*$",
        r"^(tools?|technologies?|tech\s+stack)\s*(used)?\s*:?\s*$",
    ]
    ROLE_DATE_RE = re.compile(
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|\d{4})\s*[–\-—]\s*'
        r'(present|current|\d{4}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
        re.IGNORECASE,
    )

    def __init__(self):
        super().__init__()
        self._top = [re.compile(p, re.IGNORECASE) for p in self.TOP_SECTIONS]
        self._sub = [re.compile(p, re.IGNORECASE) for p in self.SUB_SECTIONS]

    def score_line(self, line, all_lines, idx, cluster, font_clusters, median_gap):
        score, signals = super().score_line(line, all_lines, idx, cluster, font_clusters, median_gap)
        text = line.text.strip()
        wc = len(text.split())

        for pat in self._top:
            if pat.match(text):
                score += 0.55
                signals.append("cv_top")
                break

        if "cv_top" not in signals:
            for pat in self._sub:
                if pat.match(text):
                    score += 0.50
                    signals.append("cv_sub")
                    break

        if self.ROLE_DATE_RE.search(text):
            score += 0.45
            signals.append("cv_role_date")

        if (idx > 0 and line.is_bold and wc <= 6
                and not self._is_bullet(text) and not text.endswith('.')):
            prev = all_lines[idx - 1].text.strip()
            if self.ROLE_DATE_RE.search(prev):
                score += 0.30
                signals.append("cv_company")

        return score, signals

    def assign_level(self, line, signals, cluster):
        if "cv_top" in signals:
            return 2
        if "cv_sub" in signals:
            return 3
        if "cv_role_date" in signals:
            return 3
        if "cv_company" in signals:
            return 4
        return super().assign_level(line, signals, cluster)

    def adjust_levels(self, boundaries):
        if len(boundaries) < 2:
            return boundaries

        FIXED = {"cv_top", "cv_role_date", "cv_company"}
        stack: list[tuple[int, Boundary]] = []

        for b in boundaries:
            has_fixed = any(s in b.signals for s in FIXED)

            if "cv_top" in b.signals:
                b.level = 2
                stack = [(2, b)]
            elif "cv_role_date" in b.signals:
                b.level = 3
                stack = [s for s in stack if s[0] < 3]
                stack.append((3, b))
            elif "cv_company" in b.signals:
                b.level = 4
                stack = [s for s in stack if s[0] < 4]
                stack.append((4, b))
            elif "cv_sub" in b.signals:
                if stack:
                    b.level = stack[-1][0] + 1
                else:
                    b.level = 3
                stack.append((b.level, b))
            else:
                if stack and not has_fixed:
                    parent = stack[-1][0]
                    if b.level <= parent:
                        b.level = parent + 1
                stack.append((b.level, b))

        return boundaries


class BookProfile(BaseProfile):
    name = "book"

    CHAPTER_PATTERNS = [
        (r"^(?:CHAPTER|PART|BOOK)\s+(\d+|[IVXLCDM]+|[A-Z])", "book_chapter"),
        (r"^(Prologue|Epilogue|Foreword|Preface|Introduction|Afterword|Acknowledgements?)\s*$", "book_front_back"),
        (r"^(Table\s+of\s+Contents|Contents|Index|Glossary|Bibliography|References|Appendix)\s*:?\s*$", "book_structural"),
    ]

    def __init__(self):
        super().__init__()
        self._book_pats = [(re.compile(p, re.IGNORECASE), tag) for p, tag in self.CHAPTER_PATTERNS]

    def score_line(self, line, all_lines, idx, cluster, font_clusters, median_gap):
        score, signals = super().score_line(line, all_lines, idx, cluster, font_clusters, median_gap)
        for pat, tag in self._book_pats:
            if pat.match(line.text.strip()):
                score += 0.55
                signals.append(tag)
                break
        return score, signals

    def assign_level(self, line, signals, cluster):
        if "book_chapter" in signals or "book_front_back" in signals or "book_structural" in signals:
            return 1
        return super().assign_level(line, signals, cluster)


class InvoiceProfile(BaseProfile):
    name = "invoice"

    SECTION_PATTERNS = [
        (r"^(bill\s*to|ship\s*to|sold\s*to|deliver\s*to)\s*:?\s*$", "inv_address"),
        (r"^(invoice|order|receipt|po|purchase\s+order)\s*(number|no|#|date|details?)?\s*:?\s*$", "inv_header"),
        (r"^(subtotal|sub[\s-]?total|total|grand\s+total|amount\s+due|balance\s+due)\s*:?\s*$", "inv_total"),
        (r"^(payment|terms|conditions|notes?|remarks?|description|item|qty|quantity|unit\s+price)\s*:?\s*$", "inv_field"),
        (r"^(tax|vat|gst|discount|shipping|handling)\s*:?\s*$", "inv_field"),
    ]

    def __init__(self):
        super().__init__()
        self._inv_pats = [(re.compile(p, re.IGNORECASE), tag) for p, tag in self.SECTION_PATTERNS]

    def score_line(self, line, all_lines, idx, cluster, font_clusters, median_gap):
        score, signals = super().score_line(line, all_lines, idx, cluster, font_clusters, median_gap)
        for pat, tag in self._inv_pats:
            if pat.match(line.text.strip()):
                score += 0.50
                signals.append(tag)
                break
        return score, signals

    def assign_level(self, line, signals, cluster):
        if "inv_header" in signals:
            return 1
        if "inv_address" in signals or "inv_total" in signals:
            return 2
        if "inv_field" in signals:
            return 3
        return super().assign_level(line, signals, cluster)


class ReportProfile(BaseProfile):
    name = "report"

    SECTION_PATTERNS = [
        (r"^(abstract|executive\s+summary)\s*:?\s*$", "rpt_top"),
        (r"^(introduction|background|overview|scope|purpose)\s*:?\s*$", "rpt_top"),
        (r"^(methodology|methods|approach|procedure|design)\s*:?\s*$", "rpt_top"),
        (r"^(results|findings|analysis|data|observations)\s*:?\s*$", "rpt_top"),
        (r"^(discussion|interpretation|implications)\s*:?\s*$", "rpt_top"),
        (r"^(conclusion|summary|recommendations?|future\s+work)\s*:?\s*$", "rpt_top"),
        (r"^(references|bibliography|works?\s+cited|appendix|annex)\s*:?\s*$", "rpt_top"),
        (r"^(acknowledgements?|funding|conflicts?\s+of\s+interest)\s*:?\s*$", "rpt_top"),
        (r"^(literature\s+review|related\s+work|state\s+of\s+the\s+art)\s*:?\s*$", "rpt_top"),
        (r"^(limitations?|threats?\s+to\s+validity)\s*:?\s*$", "rpt_sub"),
    ]

    def __init__(self):
        super().__init__()
        self._rpt_pats = [(re.compile(p, re.IGNORECASE), tag) for p, tag in self.SECTION_PATTERNS]

    def score_line(self, line, all_lines, idx, cluster, font_clusters, median_gap):
        score, signals = super().score_line(line, all_lines, idx, cluster, font_clusters, median_gap)
        for pat, tag in self._rpt_pats:
            if pat.match(line.text.strip()):
                score += 0.50
                signals.append(tag)
                break
        return score, signals

    def assign_level(self, line, signals, cluster):
        if "rpt_top" in signals:
            return 2
        if "rpt_sub" in signals:
            return 3
        return super().assign_level(line, signals, cluster)


class LegalProfile(BaseProfile):
    name = "legal"

    SECTION_PATTERNS = [
        (r"^(?:SECTION|ARTICLE|CLAUSE|PART)\s+\w+", "legal_section"),
        (r"^(?:RECITALS?|WHEREAS|DEFINITIONS?|INTERPRETATIONS?)\s*:?\s*$", "legal_top"),
        (r"^(?:SCHEDULE|EXHIBIT|ANNEX|APPENDIX)\s+\w+", "legal_appendix"),
        (r"^(?:IN\s+WITNESS\s+WHEREOF|SIGNATURES?|EXECUTED)\s*", "legal_closing"),
        (r"^\(\s*[a-z]\s*\)\s+", "legal_sub_clause"),
        (r"^\(\s*[ivxlc]+\s*\)\s+", "legal_roman_clause"),
    ]

    def __init__(self):
        super().__init__()
        self._legal_pats = [(re.compile(p, re.IGNORECASE), tag) for p, tag in self.SECTION_PATTERNS]

    def score_line(self, line, all_lines, idx, cluster, font_clusters, median_gap):
        score, signals = super().score_line(line, all_lines, idx, cluster, font_clusters, median_gap)
        for pat, tag in self._legal_pats:
            if pat.match(line.text.strip()):
                score += 0.50
                signals.append(tag)
                break
        return score, signals

    def assign_level(self, line, signals, cluster):
        if "legal_section" in signals or "legal_top" in signals or "legal_appendix" in signals or "legal_closing" in signals:
            return 2
        if "legal_sub_clause" in signals or "legal_roman_clause" in signals:
            return 4
        return super().assign_level(line, signals, cluster)


# Profile registry — shared by DocumentClassifier and BoundaryDetector
PROFILES: dict[str, type[BaseProfile]] = {
    "generic": BaseProfile,
    "cv": CVProfile,
    "book": BookProfile,
    "invoice": InvoiceProfile,
    "report": ReportProfile,
    "legal": LegalProfile,
}


# ══════════════════════════════════════════════════════════════════════
#  AUTO-DETECT DOCUMENT TYPE
# ══════════════════════════════════════════════════════════════════════

class DocumentClassifier:

    def classify(self, lines: list[RawLine], doc_meta: dict) -> str:
        full_text = " ".join(l.text for l in lines[:100]).lower()
        num_pages = doc_meta.get("total_pages", 1)

        scores = {
            "cv": self._score_cv(full_text, lines),
            "book": self._score_book(full_text, num_pages),
            "invoice": self._score_invoice(full_text),
            "report": self._score_report(full_text),
            "legal": self._score_legal(full_text),
        }

        best = max(scores, key=scores.get)
        return best if scores[best] >= 3 else "generic"

    def _score_cv(self, text, lines):
        s = sum(1 for w in ["experience", "education", "skills", "summary",
                             "certification", "employer", "responsibilities",
                             "linkedin", "github"] if w in text)
        if re.search(r'\d{4}\s*[–\-—]\s*(present|\d{4})', text, re.IGNORECASE):
            s += 2
        first = " ".join(l.text for l in lines[:10]).lower()
        if re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', first):
            s += 1
        return s

    def _score_book(self, text, num_pages):
        s = 2 if num_pages > 20 else 0
        if re.search(r'chapter\s+\d', text, re.IGNORECASE):
            s += 3
        s += sum(1 for w in ["prologue", "epilogue", "foreword", "preface",
                              "table of contents"] if w in text)
        return s

    def _score_invoice(self, text):
        s = sum(1 for w in ["invoice", "bill to", "ship to", "subtotal",
                             "total", "payment", "qty", "unit price",
                             "amount due", "tax", "vat"] if w in text)
        if re.search(r'[\$£€]\s*\d+', text):
            s += 1
        return s

    def _score_report(self, text):
        return sum(1 for w in ["abstract", "introduction", "methodology",
                                "conclusion", "findings", "results",
                                "discussion", "references",
                                "executive summary", "recommendations"] if w in text)

    def _score_legal(self, text):
        return sum(1 for w in ["whereas", "herein", "hereinafter", "party",
                                "parties", "agreement", "contract", "clause",
                                "article", "section", "shall",
                                "in witness whereof", "indemnify",
                                "liability"] if w in text)


# ══════════════════════════════════════════════════════════════════════
#  PASS 1: BOUNDARY DETECTION
# ══════════════════════════════════════════════════════════════════════

class BoundaryDetector:

    def __init__(self, profile: BaseProfile, font_clusters: dict):
        self.profile = profile
        self.font_clusters = font_clusters
        self.font_analyzer = FontClusterAnalyzer()

    def detect(self, lines: list[RawLine]) -> list[Boundary]:
        if not lines:
            return []

        gaps = [l.gap_before for l in lines if l.gap_before > 0]
        median_gap = sorted(gaps)[len(gaps) // 2] if gaps else 0

        scored = []
        for i, line in enumerate(lines):
            cluster = self.font_analyzer.get_cluster(line.avg_font_size, self.font_clusters)
            score, signals = self.profile.score_line(
                line, lines, i, cluster, self.font_clusters, median_gap,
            )
            scored.append((line, score, signals, cluster))

        boundaries = []
        for line, score, signals, cluster in scored:
            if score >= 0.45:
                level = self.profile.assign_level(line, signals, cluster)
                boundaries.append(Boundary(
                    line_index=line.line_index, text=line.text.strip(),
                    level=level, page=line.page,
                    confidence=round(min(score, 1.0), 2), signals=signals,
                ))

        boundaries = [b for b in boundaries
                      if len(b.text.strip()) > 2
                      and not (re.match(r'^[▪•●○◆◇■□–—\-\*]\s', b.text) and b.confidence < 0.6)]
        return self.profile.adjust_levels(boundaries)


# ══════════════════════════════════════════════════════════════════════
#  PASS 2: CARVE → NEST → WALK → CHUNK
# ══════════════════════════════════════════════════════════════════════

class SectionBuilder:

    def __init__(self, config: ChunkerConfig):
        self.config = config
        self.max_chars = int(config.max_chunk_tokens * config.chars_per_token)

    def build_and_chunk(self, lines: list[RawLine], boundaries: list[Boundary],
                        doc_meta: dict) -> list[ChunkResult]:
        sections = self._carve(lines, boundaries)
        tree = self._nest(sections)
        raw: list[dict] = []
        self._walk(tree, [], raw)
        return self._postprocess(raw)

    def _carve(self, lines, boundaries):
        if not boundaries:
            pages = sorted(set(l.page for l in lines)) if lines else [1]
            return [Section("(Document)", 1, lines, pages[0], pages[-1])]

        sections = []
        first_idx = boundaries[0].line_index
        preamble = [l for l in lines if l.line_index < first_idx]
        if preamble:
            pages = sorted(set(l.page for l in preamble))
            sections.append(Section("(Preamble)", 1, preamble, pages[0], pages[-1]))

        for i, b in enumerate(boundaries):
            start = b.line_index + 1
            end = (boundaries[i + 1].line_index if i + 1 < len(boundaries)
                   else (lines[-1].line_index + 1 if lines else start))
            content = [l for l in lines if start <= l.line_index < end]
            all_sect = [l for l in lines if b.line_index <= l.line_index < end]
            pages = sorted(set(l.page for l in all_sect)) if all_sect else [b.page]
            sections.append(Section(b.text.strip(), b.level, content, pages[0], pages[-1]))
        return sections

    def _nest(self, flat):
        root: list[Section] = []
        stack: list[tuple[int, Section]] = []
        for s in flat:
            while stack and stack[-1][0] >= s.level:
                stack.pop()
            if stack:
                stack[-1][1].children.append(s)
            else:
                root.append(s)
            stack.append((s.level, s))
        return root

    def _walk(self, sections, parent_path, out):
        for s in sections:
            path = parent_path + [s.heading]
            text = "\n".join(l.text for l in s.content_lines).strip()
            pages = sorted(set(l.page for l in s.content_lines)) if s.content_lines else [s.page_start]
            if text:
                out.append({"text": text, "context_path": list(path), "pages": pages})
            if s.children:
                self._walk(s.children, path, out)

    def _postprocess(self, raw) -> list[ChunkResult]:
        expanded = []
        for rc in raw:
            if len(rc["text"]) <= self.max_chars:
                expanded.append(rc)
            else:
                for piece in self._smart_split(rc["text"]):
                    expanded.append({**rc, "text": piece})

        final: list[ChunkResult] = []
        for rc in expanded:
            chunk = self._make_chunk(rc)
            if chunk.token_estimate < self.config.min_chunk_tokens and final:
                prev = final[-1]
                same_parent = (
                    chunk.context_path[:-1] == prev.context_path[:-1]
                    if len(chunk.context_path) > 1 and len(prev.context_path) > 1
                    else chunk.context_path == prev.context_path
                )
                merged = prev.text + "\n" + chunk.text
                if same_parent and self._est(merged) <= self.config.max_chunk_tokens * 1.2:
                    prev.text = merged
                    prev.full_text = prev.context_header + "\n\n" + merged
                    prev.token_estimate = self._est(prev.full_text)
                    prev.page_numbers = sorted(set(prev.page_numbers + chunk.page_numbers))
                    continue
            final.append(chunk)

        for i, c in enumerate(final):
            c.chunk_id = f"chunk_{i:04d}"
        return final

    def _smart_split(self, text):
        paras = re.split(r'\n\n+', text)
        pieces: list[str] = []
        cur = ""
        for p in paras:
            cand = (cur + "\n\n" + p).strip() if cur else p
            if len(cand) <= self.max_chars:
                cur = cand
            else:
                if cur:
                    pieces.append(cur)
                if len(p) > self.max_chars:
                    sents = re.split(r'(?<=[.!?])\s+', p)
                    cur = ""
                    for s in sents:
                        c2 = (cur + " " + s).strip() if cur else s
                        if len(c2) <= self.max_chars:
                            cur = c2
                        else:
                            if cur:
                                pieces.append(cur)
                            cur = s
                else:
                    cur = p
        if cur:
            pieces.append(cur)
        return pieces

    def _make_chunk(self, raw) -> ChunkResult:
        path = raw["context_path"]
        hdr = "\n".join(("  " * i + f"> {h}") for i, h in enumerate(path)) if path else ""
        full = (hdr + "\n\n" + raw["text"]).strip() if hdr else raw["text"]
        return ChunkResult(
            chunk_id="", text=raw["text"], context_path=path,
            context_header=hdr, full_text=full, chunk_type="text",
            page_numbers=raw.get("pages", []),
            token_estimate=self._est(full),
        )

    def _est(self, text):
        return max(1, int(len(text) / self.config.chars_per_token))


# ══════════════════════════════════════════════════════════════════════
#  CvChunker — implements BaseChunker
# ══════════════════════════════════════════════════════════════════════

class CvChunker(BaseChunker):
    """Multi-pass chunker that auto-detects profile and produces ChunkResults."""

    def __init__(self, profile: str = "auto"):
        self._profile = profile

    def chunk(self, lines: list[RawLine], doc_meta: dict) -> list[ChunkResult]:
        config = ChunkerConfig()

        # Pass 0: Font clusters
        font_analyzer = FontClusterAnalyzer()
        font_clusters = font_analyzer.analyze(lines)

        # Pass 0.5: Auto-detect profile
        profile = self._profile
        if profile == "auto":
            classifier = DocumentClassifier()
            profile = classifier.classify(lines, doc_meta)

        active_profile = PROFILES.get(profile, BaseProfile)()

        # Pass 1: Boundaries
        detector = BoundaryDetector(active_profile, font_clusters)
        boundaries = detector.detect(lines)

        # Pass 2: Chunks
        builder = SectionBuilder(config)
        chunks = builder.build_and_chunk(lines, boundaries, doc_meta)

        # Tag each chunk with the detected profile
        for c in chunks:
            c.metadata["profile"] = active_profile.name

        return chunks

    @property
    def detected_profile(self) -> str:
        return self._profile
