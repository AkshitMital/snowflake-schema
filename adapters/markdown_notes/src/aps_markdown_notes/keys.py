"""Deterministic key generation for APS generic content ingestion.

The functions in this module mirror the v2 ingestion contract formulas. They
return business keys used for Snowflake merges; deterministic IDs are derived
from those keys but are intentionally secondary to the business keys.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .normalize import (
    blank_to_none,
    hash_markdown_text,
    normalize_body_role,
    normalize_date_component,
    normalize_date_type,
    normalize_entity_name,
    normalize_entity_type,
    normalize_hash_value,
    normalize_identifier_token,
    normalize_scope_type,
    normalize_section_path,
    normalize_source_container,
    normalize_source_object_id,
    normalize_source_path,
    normalize_source_system,
    normalize_source_uri,
    sha256_json,
    sha256_text,
)


class KeyGenerationError(ValueError):
    """Raised when the v2 key contract cannot be satisfied."""


@dataclass(frozen=True)
class ItemIdentity:
    content_item_key: str
    logical_source_key: str
    item_identity_method: str
    source_system: str
    source_object_id: str | None = None
    source_uri_normalized: str | None = None
    source_container: str | None = None
    source_path_normalized: str | None = None


def _require(value: str | None, field_name: str) -> str:
    if value is None:
        raise KeyGenerationError(f"{field_name} is required")
    return value


def _first_present(*values: str | None) -> str | None:
    for value in values:
        if value is not None:
            return value
    return None


def resolve_item_identity(
    *,
    source_system: Any,
    source_object_id: Any = None,
    normalized_source_uri: Any = None,
    source_uri: Any = None,
    source_container: Any = None,
    normalized_source_path: Any = None,
    source_path: Any = None,
) -> ItemIdentity:
    """Resolve item identity using the v2 source-object-id/URI/path order."""

    normalized_system = _require(normalize_source_system(source_system), "source_system")
    normalized_object_id = normalize_source_object_id(source_object_id)
    normalized_uri = normalize_source_uri(normalized_source_uri) or normalize_source_uri(source_uri)
    normalized_container = normalize_source_container(source_container)
    normalized_path = normalize_source_path(normalized_source_path) or normalize_source_path(source_path)

    if normalized_object_id is not None:
        identity = normalized_object_id
        method = "SOURCE_OBJECT_ID"
    elif normalized_uri is not None:
        identity = normalized_uri
        method = "NORMALIZED_URI"
    elif normalized_container is not None and normalized_path is not None:
        identity = f"{normalized_container}/{normalized_path}"
        method = "CONTAINER_PATH"
    else:
        raise KeyGenerationError(
            "content item identity requires source_object_id, normalized source_uri, "
            "or both source_container and source_path"
        )

    content_item_key = f"{normalized_system}:{identity}"
    return ItemIdentity(
        content_item_key=content_item_key,
        logical_source_key=content_item_key,
        item_identity_method=method,
        source_system=normalized_system,
        source_object_id=normalized_object_id,
        source_uri_normalized=normalized_uri,
        source_container=normalized_container,
        source_path_normalized=normalized_path,
    )


def make_content_item_key(**kwargs: Any) -> str:
    return resolve_item_identity(**kwargs).content_item_key


def make_content_version_key(
    *,
    content_item_key: str,
    source_version_id: Any = None,
    content_fingerprint: Any = None,
    source_file_hash: Any = None,
) -> str:
    version_identity = _first_present(
        normalize_source_object_id(source_version_id),
        normalize_hash_value(content_fingerprint),
        normalize_hash_value(source_file_hash),
    )
    if version_identity is None:
        raise KeyGenerationError(
            "content version identity requires source_version_id, content_fingerprint, or source_file_hash"
        )
    return f"{_require(blank_to_none(content_item_key), 'content_item_key')}:{version_identity}"


def make_content_body_key(
    *,
    content_version_key: str,
    body_role: Any,
    section_path: Any = None,
    normalized_body_hash: Any = None,
    body_hash: Any = None,
) -> str:
    role = _require(normalize_body_role(body_role), "body_role")
    section = normalize_section_path(section_path) or "document"
    hash_value = _first_present(normalize_hash_value(normalized_body_hash), normalize_hash_value(body_hash))
    if hash_value is None:
        raise KeyGenerationError("content body key requires normalized_body_hash or body_hash")
    return f"{_require(blank_to_none(content_version_key), 'content_version_key')}:body:{role}:{section}:{hash_value}"


def make_content_asset_key(
    *,
    content_version_key: str,
    source_asset_uri: Any = None,
    file_name: Any = None,
    asset_order: Any = None,
    asset_hash: Any = None,
) -> str:
    asset_identity = _first_present(
        normalize_source_uri(source_asset_uri),
        normalize_source_path(file_name),
        blank_to_none(asset_order),
    )
    if asset_identity is None:
        raise KeyGenerationError("content asset key requires source_asset_uri, file_name, or asset_order")
    hash_value = normalize_hash_value(asset_hash) or "no_hash"
    return f"{_require(blank_to_none(content_version_key), 'content_version_key')}:asset:{asset_identity}:{hash_value}"


def make_content_scope_key(
    *,
    content_version_key: str,
    scope_type: Any,
    scope_entity_id: Any = None,
    scope_entity_name: Any = None,
    scope_label: Any = None,
) -> str:
    scope = _require(normalize_scope_type(scope_type), "scope_type")
    entity_identity = _first_present(
        normalize_source_object_id(scope_entity_id),
        normalize_entity_name(scope_entity_name),
        normalize_entity_name(scope_label),
        "unscoped",
    )
    return f"{_require(blank_to_none(content_version_key), 'content_version_key')}:scope:{scope}:{entity_identity}"


def make_content_entity_link_key(
    *,
    content_version_key: str,
    entity_type: Any,
    entity_id: Any = None,
    entity_name: Any = None,
    content_body_id: Any = None,
    content_asset_id: Any = None,
    mention_start: Any = None,
    mention_end: Any = None,
) -> str:
    normalized_entity_type = _require(normalize_entity_type(entity_type), "entity_type")
    entity_identity = _first_present(normalize_source_object_id(entity_id), normalize_entity_name(entity_name))
    if entity_identity is None:
        raise KeyGenerationError("content entity link key requires entity_id or entity_name")
    anchor = _first_present(blank_to_none(content_body_id), blank_to_none(content_asset_id), "document")
    start = blank_to_none(mention_start) or "na"
    end = blank_to_none(mention_end) or "na"
    return (
        f"{_require(blank_to_none(content_version_key), 'content_version_key')}:entity:"
        f"{normalized_entity_type}:{entity_identity}:{anchor}:{start}-{end}"
    )


def make_content_date_key(
    *,
    content_version_key: str,
    date_type: Any,
    date_value: Any = None,
    timestamp_value: Any = None,
    period_start_date: Any = None,
    period_end_date: Any = None,
    date_text: Any = None,
) -> str:
    normalized_date_type = _require(normalize_date_type(date_type), "date_type")
    period_value = None
    start = normalize_date_component(period_start_date)
    end = normalize_date_component(period_end_date)
    if start is not None and end is not None:
        period_value = f"{start}/{end}"

    selected_date = _first_present(
        normalize_date_component(date_value),
        normalize_date_component(timestamp_value),
        period_value,
        normalize_date_component(date_text),
    )
    if selected_date is None:
        raise KeyGenerationError(
            "content date key requires date_value, timestamp_value, period_start/end, or date_text"
        )
    return f"{_require(blank_to_none(content_version_key), 'content_version_key')}:date:{normalized_date_type}:{selected_date}"


def make_deterministic_id(namespace: Any, business_key: str, *, length: int = 32) -> str:
    prefix = _require(normalize_identifier_token(namespace, uppercase=True), "namespace")
    key = _require(blank_to_none(business_key), "business_key")
    return f"{prefix}_{sha256_text(key)[:length]}"


def make_body_hash(markdown_text: Any) -> str:
    return hash_markdown_text(markdown_text)


def make_asset_fingerprint(asset_hashes: Iterable[Any]) -> str:
    normalized_hashes = sorted(value for value in (normalize_hash_value(item) for item in asset_hashes) if value)
    return sha256_json({"asset_hashes": normalized_hashes})


def make_content_fingerprint(*, body_hash: Any, asset_fingerprint: Any = None) -> str:
    normalized_body_hash = _require(normalize_hash_value(body_hash), "body_hash")
    normalized_asset_fingerprint = normalize_hash_value(asset_fingerprint)
    return sha256_json(
        {
            "asset_fingerprint": normalized_asset_fingerprint,
            "body_hash": normalized_body_hash,
        }
    )

