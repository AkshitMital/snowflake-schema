"""Parse resolved markdown bundles into primary and section bodies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .body_sections import BodyCandidate, create_primary_body, extract_section_bodies
from .bundle_reader import ResolvedBundle
from .keys import make_deterministic_id


@dataclass(frozen=True)
class ParsedDocument:
    document_id: str
    bundle_id: str
    title: str
    markdown_text: str
    primary_body: BodyCandidate
    section_bodies: list[BodyCandidate]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def bodies(self) -> list[BodyCandidate]:
        return [self.primary_body, *self.section_bodies]


def parse_bundle(bundle: ResolvedBundle) -> ParsedDocument:
    document_id = make_deterministic_id("content_item", bundle.bundle_id)
    primary_body = create_primary_body(document_id, bundle.markdown_text)
    section_bodies = extract_section_bodies(document_id, bundle.markdown_text, primary_body.body_id)

    return ParsedDocument(
        document_id=document_id,
        bundle_id=bundle.bundle_id,
        title=bundle.title,
        markdown_text=bundle.markdown_text,
        primary_body=primary_body,
        section_bodies=section_bodies,
        metadata={
            "markdown_path": bundle.markdown_path,
            "source_system": bundle.source.get("source_system"),
            "source_title": bundle.source.get("source_title"),
        },
    )

