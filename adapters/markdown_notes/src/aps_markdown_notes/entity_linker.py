"""Entity-link extraction for parsed APS markdown notes."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from .markdown_parser import ParsedDocument
from .normalize import collapse_whitespace, normalize_entity_name, normalize_entity_type

_GENERIC_HEADINGS = {
    "appendix",
    "commentary",
    "highlights",
    "investment updates",
    "investments",
    "market update",
    "overview",
    "portfolio",
    "portfolio update",
    "strategy",
    "summary",
}


@dataclass(frozen=True)
class EntityLinkCandidate:
    entity_type: str
    entity_id: str | None
    entity_name: str
    mention_text: str
    mention_start: int | None
    mention_end: int | None
    mention_context: str | None
    content_body_id: str
    link_role: str
    anchor_scope: str
    review_status: str
    confidence: float
    evidence_text: str
    metadata: dict[str, Any] = field(default_factory=dict)


def link_entities(
    parsed_document: ParsedDocument,
    *,
    entity_catalog: dict[str, dict[str, str]] | None = None,
) -> list[EntityLinkCandidate]:
    entity_catalog = entity_catalog or {}
    candidates: list[EntityLinkCandidate] = []
    seen: set[tuple[Any, ...]] = set()

    bodies_for_mentions = parsed_document.section_bodies or [parsed_document.primary_body]
    title_text = parsed_document.title

    for entity_type, entity_map in entity_catalog.items():
        normalized_type = normalize_entity_type(entity_type) or "UNKNOWN"
        for canonical_name, entity_id in entity_map.items():
            mentions = _find_entity_mentions(title_text, canonical_name)
            for match in mentions:
                candidate = EntityLinkCandidate(
                    entity_type=normalized_type,
                    entity_id=entity_id,
                    entity_name=canonical_name,
                    mention_text=match.group(0),
                    mention_start=match.start(),
                    mention_end=match.end(),
                    mention_context=title_text,
                    content_body_id=parsed_document.primary_body.body_id,
                    link_role="DOCUMENT_SUBJECT",
                    anchor_scope="DOCUMENT",
                    review_status="AUTO_ACCEPTED",
                    confidence=0.95,
                    evidence_text="Exact catalog entity match in document title.",
                    metadata={"source": "title"},
                )
                _append_unique(candidates, seen, candidate)

            for body in bodies_for_mentions:
                body_mentions = _find_entity_mentions(body.text, canonical_name)
                for match in body_mentions:
                    heading_match = (
                        body.heading
                        and normalize_entity_name(body.heading) == normalize_entity_name(canonical_name)
                        and match.start() <= len(body.heading) + 4
                    )
                    candidate = EntityLinkCandidate(
                        entity_type=normalized_type,
                        entity_id=entity_id,
                        entity_name=canonical_name,
                        mention_text=match.group(0),
                        mention_start=match.start(),
                        mention_end=match.end(),
                        mention_context=_context_window(body.text, match.start(), match.end()),
                        content_body_id=body.body_id,
                        link_role="PRIMARY_SUBJECT" if heading_match else "MENTION",
                        anchor_scope="SECTION" if body.section_path else "DOCUMENT",
                        review_status="AUTO_ACCEPTED",
                        confidence=0.98 if heading_match else 0.85,
                        evidence_text="Exact catalog entity match in body text.",
                        metadata={"section_path": body.section_path},
                    )
                    _append_unique(candidates, seen, candidate)

    known_by_body = {
        candidate.content_body_id
        for candidate in candidates
        if candidate.link_role in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"}
    }
    for body in parsed_document.section_bodies:
        if body.body_id in known_by_body:
            continue
        heading = collapse_whitespace(body.heading)
        if not heading or normalize_entity_name(heading) in _GENERIC_HEADINGS:
            continue
        if body.section_path and "/" not in body.section_path and normalize_entity_name(heading) == normalize_entity_name(parsed_document.title):
            continue
        inferred_type = _infer_unresolved_entity_type(body.section_path, heading)
        unresolved = EntityLinkCandidate(
            entity_type=inferred_type,
            entity_id=None,
            entity_name=heading,
            mention_text=heading,
            mention_start=0,
            mention_end=len(heading),
            mention_context=body.text[:200],
            content_body_id=body.body_id,
            link_role="PRIMARY_SUBJECT",
            anchor_scope="SECTION",
            review_status="NEEDS_REVIEW",
            confidence=0.35 if inferred_type == "UNKNOWN" else 0.55,
            evidence_text="Section heading looks entity-like but did not resolve in the catalog.",
            metadata={"section_path": body.section_path},
        )
        _append_unique(candidates, seen, unresolved)

    return candidates


def _append_unique(
    candidates: list[EntityLinkCandidate],
    seen: set[tuple[Any, ...]],
    candidate: EntityLinkCandidate,
) -> None:
    identity = (
        candidate.entity_type,
        candidate.entity_id,
        normalize_entity_name(candidate.entity_name),
        candidate.content_body_id,
        candidate.mention_start,
        candidate.mention_end,
    )
    if identity in seen:
        return
    seen.add(identity)
    candidates.append(candidate)


def _find_entity_mentions(text: str, canonical_name: str) -> list[re.Match[str]]:
    if not text:
        return []
    pattern = re.compile(rf"(?<!\w){re.escape(canonical_name)}(?!\w)", re.IGNORECASE)
    return list(pattern.finditer(text))


def _context_window(text: str, start: int, end: int, *, width: int = 60) -> str:
    context_start = max(0, start - width)
    context_end = min(len(text), end + width)
    return text[context_start:context_end]


def _infer_unresolved_entity_type(section_path: str | None, heading: str) -> str:
    normalized_path = normalize_entity_name(section_path or "") or ""
    normalized_heading = normalize_entity_name(heading) or ""
    if "investment" in normalized_path:
        return "INVESTMENT"
    if "portfolio" in normalized_path or "fund" in normalized_heading:
        return "PORTFOLIO"
    if any(token in normalized_heading for token in ("weekly", "monthly", "strategy")):
        return "STRATEGY"
    return "UNKNOWN"
