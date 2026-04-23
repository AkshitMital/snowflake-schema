"""Normalization helpers for deterministic APS content keys.

These helpers implement the project-standard normalization required by the v2
ingestion contract before hashing or key generation: trim whitespace, collapse
internal whitespace where safe, URL-decode paths/URIs, normalize path separators,
and apply predictable case handling for fallback identities.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

_WHITESPACE_RE = re.compile(r"\s+")
_SLASH_RE = re.compile(r"/+")
_UNDERSCORE_RE = re.compile(r"_+")
_BLANK_LINE_RE = re.compile(r"\n{3,}")


def blank_to_none(value: Any) -> str | None:
    """Return a stripped string, or None when the value is absent/blank."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def collapse_whitespace(value: Any) -> str | None:
    """Trim a string and collapse all whitespace runs to a single space."""

    text = blank_to_none(value)
    if text is None:
        return None
    return _WHITESPACE_RE.sub(" ", text)


def normalize_identifier_token(value: Any, *, uppercase: bool = True) -> str | None:
    """Normalize enum-like values such as source systems and entity types."""

    text = collapse_whitespace(unquote(str(value))) if value is not None else None
    if text is None:
        return None
    text = text.replace("-", "_").replace(" ", "_")
    text = _UNDERSCORE_RE.sub("_", text)
    text = text.strip("_")
    return text.upper() if uppercase else text.lower()


def normalize_source_system(value: Any) -> str | None:
    return normalize_identifier_token(value, uppercase=True)


def normalize_source_object_id(value: Any) -> str | None:
    """Normalize stable source IDs without forcing case changes.

    Source object IDs can be case-sensitive in some systems, so we strip,
    URL-decode, and collapse whitespace but preserve case.
    """

    return collapse_whitespace(unquote(str(value))) if value is not None else None


def normalize_source_container(value: Any) -> str | None:
    return normalize_source_path(value)


def normalize_source_path(value: Any) -> str | None:
    """Normalize path-like fallback identities.

    Path fallback identities are lower-cased because exported markdown paths are
    commonly unstable in case, slash style, and URL encoding.
    """

    text = blank_to_none(value)
    if text is None:
        return None

    text = unquote(text).replace("\\", "/")
    text = _SLASH_RE.sub("/", text)
    text = text.strip().strip("/")

    parts: list[str] = []
    for raw_part in text.split("/"):
        part = collapse_whitespace(raw_part)
        if part in (None, "", "."):
            continue
        parts.append(part.lower())
    return "/".join(parts) if parts else None


def normalize_source_uri(value: Any) -> str | None:
    """Normalize a source URI for stable fallback item identity.

    Query strings and fragments are omitted because export/download links often
    include transient parameters that should not create new content identities.
    """

    text = blank_to_none(value)
    if text is None:
        return None

    text = unquote(text).replace("\\", "/")
    parsed = urlsplit(text)
    if parsed.scheme and parsed.netloc:
        path = normalize_source_path(parsed.path) or ""
        normalized_path = f"/{path}" if path else ""
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), normalized_path, "", ""))

    return normalize_source_path(text)


def normalize_entity_name(value: Any) -> str | None:
    text = collapse_whitespace(unquote(str(value))) if value is not None else None
    return text.lower() if text else None


def normalize_body_role(value: Any) -> str | None:
    return normalize_identifier_token(value, uppercase=True)


def normalize_scope_type(value: Any) -> str | None:
    return normalize_identifier_token(value, uppercase=True)


def normalize_entity_type(value: Any) -> str | None:
    return normalize_identifier_token(value, uppercase=True)


def normalize_date_type(value: Any) -> str | None:
    return normalize_identifier_token(value, uppercase=True)


def normalize_section_path(value: Any) -> str | None:
    return normalize_source_path(value)


def normalize_hash_value(value: Any) -> str | None:
    text = blank_to_none(value)
    return text.lower() if text else None


def normalize_date_component(value: Any) -> str | None:
    return collapse_whitespace(value)


def normalize_markdown_text(value: Any) -> str:
    """Normalize markdown text for stable body hashing.

    This preserves material line structure while ignoring line-ending drift,
    trailing whitespace, and extra leading/trailing blank lines.
    """

    if value is None:
        return ""

    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]

    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    normalized = "\n".join(lines)
    return _BLANK_LINE_RE.sub("\n\n", normalized)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def hash_markdown_text(value: Any) -> str:
    return sha256_text(normalize_markdown_text(value))

