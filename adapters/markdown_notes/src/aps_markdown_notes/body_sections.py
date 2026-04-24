"""Section extraction helpers for markdown notes."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from .keys import make_deterministic_id
from .normalize import collapse_whitespace, normalize_markdown_text

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class BodyCandidate:
    body_id: str
    body_role: str
    section_path: str | None
    parent_body_id: str | None
    heading: str | None
    text: str
    line_start: int
    line_end: int
    metadata: dict[str, Any] = field(default_factory=dict)


def create_primary_body(document_id: str, markdown_text: str) -> BodyCandidate:
    return BodyCandidate(
        body_id=make_deterministic_id("content_body", f"{document_id}:PRIMARY:document"),
        body_role="PRIMARY",
        section_path=None,
        parent_body_id=None,
        heading=None,
        text=normalize_markdown_text(markdown_text),
        line_start=1,
        line_end=max(1, len(markdown_text.splitlines()) or 1),
        metadata={"anchor_scope": "DOCUMENT"},
    )


def extract_section_bodies(document_id: str, markdown_text: str, primary_body_id: str) -> list[BodyCandidate]:
    lines = markdown_text.splitlines()
    sections: list[BodyCandidate] = []
    stack: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def finalize(end_line: int) -> None:
        nonlocal current
        if current is None:
            return
        text = normalize_markdown_text("\n".join(current["lines"]))
        sections.append(
            BodyCandidate(
                body_id=current["body_id"],
                body_role="SECTION",
                section_path=current["section_path"],
                parent_body_id=current["parent_body_id"],
                heading=current["heading"],
                text=text,
                line_start=current["line_start"],
                line_end=end_line,
                metadata={
                    "anchor_scope": "SECTION",
                    "heading_level": current["level"],
                },
            )
        )
        current = None

    for line_number, line in enumerate(lines, start=1):
        heading_match = _HEADING_RE.match(line)
        if heading_match:
            finalize(line_number - 1)

            level = len(heading_match.group(1))
            heading = collapse_whitespace(heading_match.group(2)) or "section"

            while stack and stack[-1]["level"] >= level:
                stack.pop()

            parent_section_path = stack[-1]["section_path"] if stack else None
            parent_body_id = stack[-1]["body_id"] if stack else primary_body_id
            slug = slugify_heading(heading)
            section_path = f"{parent_section_path}/{slug}" if parent_section_path else slug
            body_id = make_deterministic_id("content_body", f"{document_id}:SECTION:{section_path}")

            current = {
                "body_id": body_id,
                "heading": heading,
                "level": level,
                "line_start": line_number,
                "lines": [line],
                "parent_body_id": parent_body_id,
                "section_path": section_path,
            }
            stack.append(
                {
                    "body_id": body_id,
                    "level": level,
                    "section_path": section_path,
                }
            )
            continue

        if current is not None:
            current["lines"].append(line)

    finalize(len(lines) or 1)
    return sections


def slugify_heading(heading: str) -> str:
    normalized = (collapse_whitespace(heading) or "section").lower()
    slug = _NON_ALNUM_RE.sub("-", normalized).strip("-")
    return slug or "section"

