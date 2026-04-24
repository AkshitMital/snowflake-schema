"""Date-candidate extraction for parsed APS markdown notes."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import re
from typing import Any

from .bundle_reader import ResolvedBundle
from .markdown_parser import ParsedDocument

_MEETING_DATE_RE = re.compile(r"(?im)\bmeeting date\s*:\s*(?P<date>[^\n]+)")
_PERIOD_RANGE_RE = re.compile(
    r"(?im)\b(?:report period|period)\s*:\s*(?P<start>[^\n]+?)\s+(?:to|through|-)\s+(?P<end>[^\n]+)"
)
_PERIOD_END_RE = re.compile(r"(?im)\b(?:for the period ending|period ending)\s+(?P<date>[^\n]+)")
_EFFECTIVE_DATE_RE = re.compile(r"(?im)\b(?:as of|effective date)\s*[:\-]?\s*(?P<date>[^\n]+)")


@dataclass(frozen=True)
class DateCandidate:
    date_type: str
    date_role: str
    date_value: str | None
    timestamp_value: str | None
    period_start_date: str | None
    period_end_date: str | None
    date_text: str | None
    content_body_id: str | None
    is_primary_candidate: bool
    confidence: float
    evidence_text: str
    metadata: dict[str, Any] = field(default_factory=dict)


def extract_date_candidates(bundle: ResolvedBundle, parsed_document: ParsedDocument) -> list[DateCandidate]:
    candidates: list[DateCandidate] = []
    seen: set[tuple[Any, ...]] = set()

    _append_unique(
        candidates,
        seen,
        _metadata_date_candidate("SOURCE_CREATED", bundle.source.get("source_created_at"), "Source created timestamp."),
    )
    _append_unique(
        candidates,
        seen,
        _metadata_date_candidate("SOURCE_MODIFIED", bundle.source.get("source_modified_at"), "Source modified timestamp."),
    )
    _append_unique(
        candidates,
        seen,
        _metadata_date_candidate("INGESTED", bundle.ingested_at, "Bundle ingestion timestamp."),
    )

    bodies_for_dates = parsed_document.section_bodies or [parsed_document.primary_body]
    for body in bodies_for_dates:
        _extract_body_date_candidates(body.text, body.body_id, candidates, seen)

    _extract_body_date_candidates(parsed_document.title, parsed_document.primary_body.body_id, candidates, seen, title_only=True)
    return [candidate for candidate in candidates if candidate is not None]


def _extract_body_date_candidates(
    text: str,
    content_body_id: str,
    candidates: list[DateCandidate],
    seen: set[tuple[Any, ...]],
    *,
    title_only: bool = False,
) -> None:
    for match in _MEETING_DATE_RE.finditer(text):
        date_value = _parse_date(match.group("date"))
        _append_unique(
            candidates,
            seen,
            DateCandidate(
                date_type="MEETING_DATE",
                date_role="TEXT_EXTRACTED",
                date_value=date_value,
                timestamp_value=None,
                period_start_date=None,
                period_end_date=None,
                date_text=match.group("date").strip(),
                content_body_id=content_body_id,
                is_primary_candidate=True,
                confidence=0.95,
                evidence_text="Meeting date extracted from explicit label.",
            ),
        )

    for match in _PERIOD_RANGE_RE.finditer(text):
        start = _parse_date(match.group("start"))
        end = _parse_date(match.group("end"))
        _append_unique(
            candidates,
            seen,
            DateCandidate(
                date_type="REPORT_PERIOD_START",
                date_role="TEXT_EXTRACTED",
                date_value=start,
                timestamp_value=None,
                period_start_date=start,
                period_end_date=end,
                date_text=match.group("start").strip(),
                content_body_id=content_body_id,
                is_primary_candidate=False,
                confidence=0.9,
                evidence_text="Report period start extracted from explicit range.",
            ),
        )
        _append_unique(
            candidates,
            seen,
            DateCandidate(
                date_type="REPORT_PERIOD_END",
                date_role="TEXT_EXTRACTED",
                date_value=end,
                timestamp_value=None,
                period_start_date=start,
                period_end_date=end,
                date_text=match.group("end").strip(),
                content_body_id=content_body_id,
                is_primary_candidate=not title_only,
                confidence=0.9,
                evidence_text="Report period end extracted from explicit range.",
            ),
        )

    for match in _PERIOD_END_RE.finditer(text):
        date_value = _parse_date(match.group("date"))
        _append_unique(
            candidates,
            seen,
            DateCandidate(
                date_type="REPORT_PERIOD_END",
                date_role="TEXT_EXTRACTED",
                date_value=date_value,
                timestamp_value=None,
                period_start_date=None,
                period_end_date=None,
                date_text=match.group("date").strip(),
                content_body_id=content_body_id,
                is_primary_candidate=not title_only,
                confidence=0.88,
                evidence_text="Report period end extracted from explicit label.",
            ),
        )

    for match in _EFFECTIVE_DATE_RE.finditer(text):
        date_value = _parse_date(match.group("date"))
        _append_unique(
            candidates,
            seen,
            DateCandidate(
                date_type="EFFECTIVE_DATE_CANDIDATE",
                date_role="TEXT_EXTRACTED",
                date_value=date_value,
                timestamp_value=None,
                period_start_date=None,
                period_end_date=None,
                date_text=match.group("date").strip(),
                content_body_id=content_body_id,
                is_primary_candidate=title_only,
                confidence=0.8,
                evidence_text="Effective-date candidate extracted from explicit text.",
            ),
        )


def _metadata_date_candidate(date_type: str, value: Any, evidence_text: str) -> DateCandidate | None:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    return DateCandidate(
        date_type=date_type,
        date_role="SOURCE_METADATA",
        date_value=None,
        timestamp_value=text_value,
        period_start_date=None,
        period_end_date=None,
        date_text=text_value,
        content_body_id=None,
        is_primary_candidate=date_type == "INGESTED",
        confidence=0.95,
        evidence_text=evidence_text,
    )


def _append_unique(
    candidates: list[DateCandidate],
    seen: set[tuple[Any, ...]],
    candidate: DateCandidate | None,
) -> None:
    if candidate is None:
        return
    identity = (
        candidate.date_type,
        candidate.date_value,
        candidate.timestamp_value,
        candidate.period_start_date,
        candidate.period_end_date,
        candidate.content_body_id,
    )
    if identity in seen:
        return
    seen.add(identity)
    candidates.append(candidate)


def _parse_date(raw_value: str) -> str:
    text = raw_value.strip().rstrip(".")
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text

