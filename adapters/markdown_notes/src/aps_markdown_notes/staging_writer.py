"""Stage-row generation and local merge simulation for APS content ingestion.

This module owns the adapter-side staging contract for Checkpoint C:

- build deterministic stage rows for the `CONTENT_*` tables
- serialize staging artifacts for inspection
- validate stage-batch conflicts using business keys
- simulate the v2 merge order locally so tests can prove behavior without a
  Snowflake runtime
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .asset_extractor import AssetCandidate
from .bundle_reader import ResolvedBundle
from .date_extractor import DateCandidate
from .entity_linker import EntityLinkCandidate
from .keys import (
    KeyGenerationError,
    ItemIdentity,
    make_asset_fingerprint,
    make_body_hash,
    make_content_asset_key,
    make_content_body_key,
    make_content_date_key,
    make_content_entity_link_key,
    make_content_fingerprint,
    make_content_item_key,
    make_content_scope_key,
    make_content_version_key,
    make_deterministic_id,
    resolve_item_identity,
)
from .markdown_parser import ParsedDocument
from .normalize import canonical_json, normalize_hash_value
from .scope_classifier import ScopeCandidate


StageRow = dict[str, Any]


class StageValidationError(ValueError):
    """Raised when a staging batch fails business-key validation."""

    def __init__(self, issues: list["StageValidationIssue"]):
        self.issues = issues
        summary = "; ".join(f"{issue.issue_code}:{issue.detail}" for issue in issues)
        super().__init__(summary)


@dataclass(frozen=True)
class StageValidationIssue:
    issue_code: str
    table_name: str
    business_key: str
    detail: str
    severity: str = "ERROR"
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class StagingBatch:
    ingestion_run_id: str
    content_items: list[StageRow] = field(default_factory=list)
    content_versions: list[StageRow] = field(default_factory=list)
    content_bodies: list[StageRow] = field(default_factory=list)
    content_assets: list[StageRow] = field(default_factory=list)
    content_scopes: list[StageRow] = field(default_factory=list)
    content_entity_links: list[StageRow] = field(default_factory=list)
    content_dates: list[StageRow] = field(default_factory=list)

    def table_rows(self) -> dict[str, list[StageRow]]:
        return {
            "STAGE_CONTENT_ITEMS": self.content_items,
            "STAGE_CONTENT_VERSIONS": self.content_versions,
            "STAGE_CONTENT_BODIES": self.content_bodies,
            "STAGE_CONTENT_ASSETS": self.content_assets,
            "STAGE_CONTENT_SCOPES": self.content_scopes,
            "STAGE_CONTENT_ENTITY_LINKS": self.content_entity_links,
            "STAGE_CONTENT_DATES": self.content_dates,
        }

    def to_jsonl(self, output_dir: str | Path) -> dict[str, Path]:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        written_files: dict[str, Path] = {}
        for table_name, rows in self.table_rows().items():
            file_path = output_path / f"{table_name.lower()}.jsonl"
            with file_path.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(canonical_json(row))
                    handle.write("\n")
            written_files[table_name] = file_path
        return written_files


@dataclass
class MergeState:
    content_items: dict[str, StageRow] = field(default_factory=dict)
    content_versions: dict[str, StageRow] = field(default_factory=dict)
    content_item_current_version: dict[str, StageRow] = field(default_factory=dict)
    content_bodies: dict[str, StageRow] = field(default_factory=dict)
    content_assets: dict[str, StageRow] = field(default_factory=dict)
    content_scopes: dict[str, StageRow] = field(default_factory=dict)
    content_entity_links: dict[str, StageRow] = field(default_factory=dict)
    content_dates: dict[str, StageRow] = field(default_factory=dict)

    def counts(self) -> dict[str, int]:
        return {
            "content_items": len(self.content_items),
            "content_versions": len(self.content_versions),
            "content_item_current_version": len(self.content_item_current_version),
            "content_bodies": len(self.content_bodies),
            "content_assets": len(self.content_assets),
            "content_scopes": len(self.content_scopes),
            "content_entity_links": len(self.content_entity_links),
            "content_dates": len(self.content_dates),
        }


def build_staging_batch(
    *,
    bundle: ResolvedBundle,
    parsed_document: ParsedDocument,
    assets: list[AssetCandidate],
    scopes: list[ScopeCandidate],
    entity_links: list[EntityLinkCandidate],
    dates: list[DateCandidate],
    ingestion_run_id: str,
    adapter_name: str = "aps_markdown_notes",
    adapter_version: str = "0.1.0",
    raw_payload_location: str | None = None,
    normalized_payload_location: str | None = None,
    promote_to_current: bool = True,
    version_reason: str | None = None,
    current_selection_reason: str | None = None,
) -> StagingBatch:
    now = _coalesce_timestamp(bundle.ingested_at) or _utc_now_isoformat()
    item_identity = resolve_item_identity(
        source_system=bundle.source.get("source_system"),
        source_object_id=bundle.source.get("source_object_id"),
        normalized_source_uri=bundle.source.get("source_uri"),
        source_uri=bundle.source.get("source_uri"),
        source_container=bundle.source.get("source_container"),
        normalized_source_path=bundle.source.get("source_path"),
        source_path=bundle.source.get("source_path"),
    )
    content_item_key = item_identity.content_item_key
    content_id = make_deterministic_id("content", content_item_key)

    body_rows, parsed_body_id_to_stage_row = _build_body_rows(
        parsed_document=parsed_document,
        content_id=content_id,
        content_item_key=content_item_key,
        body_format="MARKDOWN",
        ingestion_run_id=ingestion_run_id,
        staged_at=now,
    )
    primary_body_row = next(row for row in body_rows if row["IS_PRIMARY_BODY"])
    body_hash = primary_body_row["NORMALIZED_BODY_HASH"]
    asset_fingerprint = make_asset_fingerprint(asset.asset_hash for asset in assets)
    content_fingerprint = make_content_fingerprint(body_hash=body_hash, asset_fingerprint=asset_fingerprint)
    content_version_key = make_content_version_key(
        content_item_key=content_item_key,
        source_version_id=bundle.source.get("source_version_id"),
        content_fingerprint=content_fingerprint,
        source_file_hash=bundle.content.get("source_file_hash"),
    )
    content_version_id = make_deterministic_id("content_version", content_version_key)

    # Bodies need the finalized version identifiers and keys.
    for row in body_rows:
        row["CONTENT_VERSION_ID"] = content_version_id
        row["CONTENT_VERSION_KEY"] = content_version_key
        row["CONTENT_BODY_KEY"] = make_content_body_key(
            content_version_key=content_version_key,
            body_role=row["BODY_ROLE"],
            section_path=row["SECTION_PATH"],
            normalized_body_hash=row["NORMALIZED_BODY_HASH"],
            body_hash=row["BODY_HASH"],
        )
        row["CONTENT_BODY_ID"] = make_deterministic_id("content_body", row["CONTENT_BODY_KEY"])
    parent_id_to_final_key = {
        original_body_id: row["CONTENT_BODY_KEY"]
        for original_body_id, row in parsed_body_id_to_stage_row.items()
    }
    for row in body_rows:
        original_parent_id = row.pop("_ORIGINAL_PARENT_BODY_ID", None)
        row["PARENT_CONTENT_BODY_KEY"] = parent_id_to_final_key.get(original_parent_id)
        row["STAGE_ROW_HASH"], row["STAGE_PAYLOAD_HASH"] = _stage_hashes(row)

    parsed_body_id_to_final_row = {
        original_body_id: next(
            candidate for candidate in body_rows if candidate["SECTION_PATH"] == row["SECTION_PATH"] and candidate["BODY_ROLE"] == row["BODY_ROLE"]
        )
        for original_body_id, row in parsed_body_id_to_stage_row.items()
    }

    asset_rows, asset_id_map = _build_asset_rows(
        assets=assets,
        body_id_map=parsed_body_id_to_final_row,
        content_id=content_id,
        content_item_key=content_item_key,
        content_version_id=content_version_id,
        content_version_key=content_version_key,
        ingestion_run_id=ingestion_run_id,
        staged_at=now,
    )
    scope_rows = _build_scope_rows(
        scopes=scopes,
        content_id=content_id,
        content_item_key=content_item_key,
        content_version_id=content_version_id,
        content_version_key=content_version_key,
        ingestion_run_id=ingestion_run_id,
        staged_at=now,
    )
    entity_link_rows = _build_entity_link_rows(
        entity_links=entity_links,
        body_id_map=parsed_body_id_to_final_row,
        asset_id_map=asset_id_map,
        content_id=content_id,
        content_item_key=content_item_key,
        content_version_id=content_version_id,
        content_version_key=content_version_key,
        ingestion_run_id=ingestion_run_id,
        staged_at=now,
    )
    date_rows = _build_date_rows(
        dates=dates,
        body_id_map=parsed_body_id_to_final_row,
        content_id=content_id,
        content_item_key=content_item_key,
        content_version_id=content_version_id,
        content_version_key=content_version_key,
        ingestion_run_id=ingestion_run_id,
        staged_at=now,
    )

    content_item_row: StageRow = {
        "INGESTION_RUN_ID": ingestion_run_id,
        "CONTENT_ID": content_id,
        "CONTENT_ITEM_KEY": content_item_key,
        "LOGICAL_SOURCE_KEY": item_identity.logical_source_key,
        "ITEM_IDENTITY_METHOD": item_identity.item_identity_method,
        "SOURCE_SYSTEM": item_identity.source_system,
        "SOURCE_OBJECT_ID": item_identity.source_object_id,
        "SOURCE_CONTAINER": bundle.source.get("source_container"),
        "SOURCE_PATH": bundle.source.get("source_path"),
        "SOURCE_PATH_NORMALIZED": item_identity.source_path_normalized,
        "SOURCE_URI": bundle.source.get("source_uri"),
        "SOURCE_URI_NORMALIZED": item_identity.source_uri_normalized,
        "SOURCE_REFERENCE": bundle.source.get("source_reference"),
        "SOURCE_TYPE": bundle.source.get("source_type"),
        "SOURCE_SUBTYPE": bundle.source.get("source_subtype"),
        "SOURCE_TITLE": bundle.source.get("source_title") or parsed_document.title,
        "SOURCE_PROVENANCE": bundle.source.get("provenance") or bundle.source.get("metadata") or {},
        "SOURCE_CREATED_AT": _coalesce_timestamp(bundle.source.get("source_created_at")),
        "SOURCE_MODIFIED_AT": _coalesce_timestamp(bundle.source.get("source_modified_at")),
        "FIRST_SEEN_AT": now,
        "LAST_SEEN_AT": now,
        "CANONICAL_CONTENT_HASH": content_fingerprint,
        "DUPLICATE_OF_CONTENT_ID": None,
        "REVIEW_STATUS": "UNREVIEWED",
        "STATUS_REASON": None,
        "IS_ACTIVE": True,
        "METADATA": {
            "adapter_name": adapter_name,
            "adapter_version": adapter_version,
            "bundle_id": bundle.bundle_id,
            "markdown_path": bundle.markdown_path,
        },
        "STAGED_AT": now,
    }
    content_item_row["STAGE_ROW_HASH"], content_item_row["STAGE_PAYLOAD_HASH"] = _stage_hashes(content_item_row)

    version_reason = version_reason or _default_version_reason(bundle=bundle, promote_to_current=promote_to_current)
    current_selection_reason = current_selection_reason or version_reason
    content_version_row: StageRow = {
        "INGESTION_RUN_ID": ingestion_run_id,
        "CONTENT_VERSION_ID": content_version_id,
        "CONTENT_ID": content_id,
        "CONTENT_ITEM_KEY": content_item_key,
        "CONTENT_VERSION_KEY": content_version_key,
        "VERSION_NUMBER_CANDIDATE": 1,
        "VERSION_REASON": version_reason,
        "RAW_RECORD_ID": bundle.bundle_id,
        "RAW_PAYLOAD_LOCATION": raw_payload_location,
        "NORMALIZED_PAYLOAD_LOCATION": normalized_payload_location,
        "SOURCE_EXPORT_ID": bundle.source.get("source_export_id"),
        "SOURCE_VERSION_ID": bundle.source.get("source_version_id"),
        "SOURCE_EXPORTED_AT": _coalesce_timestamp(bundle.source.get("source_exported_at")),
        "SOURCE_MODIFIED_AT": _coalesce_timestamp(bundle.source.get("source_modified_at")),
        "ADAPTER_NAME": adapter_name,
        "ADAPTER_VERSION": adapter_version,
        "SOURCE_FILE_HASH": normalize_hash_value(bundle.content.get("source_file_hash")),
        "BODY_HASH": normalize_hash_value(body_hash),
        "ASSET_FINGERPRINT": normalize_hash_value(asset_fingerprint),
        "CONTENT_FINGERPRINT": normalize_hash_value(content_fingerprint),
        "VERSION_STATUS": "CURRENT" if promote_to_current else "CANDIDATE",
        "PROMOTE_TO_CURRENT": promote_to_current,
        "CURRENT_SELECTION_REASON": current_selection_reason,
        "PROPOSED_CURRENT_FROM_AT": now if promote_to_current else None,
        "METADATA": {
            "bundle_id": bundle.bundle_id,
            "bundle_title": parsed_document.title,
        },
        "STAGED_AT": now,
    }
    content_version_row["STAGE_ROW_HASH"], content_version_row["STAGE_PAYLOAD_HASH"] = _stage_hashes(content_version_row)

    return StagingBatch(
        ingestion_run_id=ingestion_run_id,
        content_items=[content_item_row],
        content_versions=[content_version_row],
        content_bodies=body_rows,
        content_assets=asset_rows,
        content_scopes=scope_rows,
        content_entity_links=entity_link_rows,
        content_dates=date_rows,
    )


def validate_staging_batch(batch: StagingBatch) -> list[StageValidationIssue]:
    issues: list[StageValidationIssue] = []
    table_defs = {
        "STAGE_CONTENT_ITEMS": (batch.content_items, "CONTENT_ITEM_KEY"),
        "STAGE_CONTENT_VERSIONS": (batch.content_versions, "CONTENT_VERSION_KEY"),
        "STAGE_CONTENT_BODIES": (batch.content_bodies, "CONTENT_BODY_KEY"),
        "STAGE_CONTENT_ASSETS": (batch.content_assets, "CONTENT_ASSET_KEY"),
        "STAGE_CONTENT_SCOPES": (batch.content_scopes, "CONTENT_SCOPE_KEY"),
        "STAGE_CONTENT_ENTITY_LINKS": (batch.content_entity_links, "CONTENT_ENTITY_LINK_KEY"),
        "STAGE_CONTENT_DATES": (batch.content_dates, "CONTENT_DATE_KEY"),
    }
    for table_name, (rows, business_key) in table_defs.items():
        issues.extend(_detect_conflicting_duplicates(table_name, rows, business_key))

    issues.extend(_validate_primary_body(batch))
    issues.extend(_validate_current_version_candidates(batch))
    issues.extend(_validate_parent_references(batch))
    return issues


def require_valid_staging_batch(batch: StagingBatch) -> None:
    issues = validate_staging_batch(batch)
    if issues:
        raise StageValidationError(issues)


def simulate_merge_batch(state: MergeState | None, batch: StagingBatch) -> MergeState:
    require_valid_staging_batch(batch)
    state = state or MergeState()
    deduped = _dedupe_identical_stage_rows(batch)

    # Step 2: merge items.
    for row in deduped.content_items:
        existing = state.content_items.get(row["CONTENT_ITEM_KEY"])
        if existing:
            existing.update(
                {
                    "SOURCE_CONTAINER": row["SOURCE_CONTAINER"],
                    "SOURCE_PATH": row["SOURCE_PATH"],
                    "SOURCE_PATH_NORMALIZED": row["SOURCE_PATH_NORMALIZED"],
                    "SOURCE_URI": row["SOURCE_URI"],
                    "SOURCE_URI_NORMALIZED": row["SOURCE_URI_NORMALIZED"],
                    "SOURCE_REFERENCE": row["SOURCE_REFERENCE"],
                    "SOURCE_TITLE": row["SOURCE_TITLE"],
                    "SOURCE_PROVENANCE": row["SOURCE_PROVENANCE"],
                    "SOURCE_CREATED_AT": row["SOURCE_CREATED_AT"],
                    "SOURCE_MODIFIED_AT": row["SOURCE_MODIFIED_AT"],
                    "LAST_SEEN_AT": row["LAST_SEEN_AT"],
                    "CANONICAL_CONTENT_HASH": row["CANONICAL_CONTENT_HASH"],
                    "METADATA": row["METADATA"],
                    "UPDATED_AT": row["STAGED_AT"],
                }
            )
        else:
            inserted = dict(row)
            inserted["CREATED_AT"] = row["STAGED_AT"]
            inserted["UPDATED_AT"] = row["STAGED_AT"]
            state.content_items[row["CONTENT_ITEM_KEY"]] = inserted

    # Step 3: merge versions.
    promoted_version_keys_by_item: dict[str, str] = {}
    for row in deduped.content_versions:
        existing = state.content_versions.get(row["CONTENT_VERSION_KEY"])
        if existing:
            existing.update(
                {
                    "SOURCE_EXPORT_ID": row["SOURCE_EXPORT_ID"],
                    "SOURCE_VERSION_ID": row["SOURCE_VERSION_ID"],
                    "SOURCE_EXPORTED_AT": row["SOURCE_EXPORTED_AT"],
                    "SOURCE_MODIFIED_AT": row["SOURCE_MODIFIED_AT"],
                    "RAW_PAYLOAD_LOCATION": row["RAW_PAYLOAD_LOCATION"],
                    "NORMALIZED_PAYLOAD_LOCATION": row["NORMALIZED_PAYLOAD_LOCATION"],
                    "ADAPTER_NAME": row["ADAPTER_NAME"],
                    "ADAPTER_VERSION": row["ADAPTER_VERSION"],
                    "SOURCE_FILE_HASH": row["SOURCE_FILE_HASH"],
                    "BODY_HASH": row["BODY_HASH"],
                    "ASSET_FINGERPRINT": row["ASSET_FINGERPRINT"],
                    "CONTENT_FINGERPRINT": row["CONTENT_FINGERPRINT"],
                    "UPDATED_AT": row["STAGED_AT"],
                    "METADATA": row["METADATA"],
                }
            )
            is_duplicate = bool(existing.get("DUPLICATE_OF_CONTENT_VERSION_ID"))
            if row["PROMOTE_TO_CURRENT"] and not is_duplicate:
                promoted_version_keys_by_item[row["CONTENT_ITEM_KEY"]] = row["CONTENT_VERSION_KEY"]
            continue

        existing_versions_same_item = [
            version for version in state.content_versions.values() if version["CONTENT_ITEM_KEY"] == row["CONTENT_ITEM_KEY"]
        ]
        duplicate_of = next(
            (
                version
                for version in existing_versions_same_item
                if version.get("CONTENT_FINGERPRINT") == row["CONTENT_FINGERPRINT"]
                and version["CONTENT_VERSION_KEY"] != row["CONTENT_VERSION_KEY"]
            ),
            None,
        )
        version_number = (max((version["VERSION_NUMBER"] for version in existing_versions_same_item), default=0) + 1)
        inserted = dict(row)
        inserted["VERSION_NUMBER"] = version_number
        inserted["CREATED_AT"] = row["STAGED_AT"]
        inserted["UPDATED_AT"] = row["STAGED_AT"]
        inserted["PREVIOUS_CONTENT_VERSION_ID"] = None
        inserted["SUPERSEDED_BY_CONTENT_VERSION_ID"] = None
        inserted["DUPLICATE_OF_CONTENT_VERSION_ID"] = None
        inserted["DUPLICATE_REASON"] = None
        inserted["IS_CURRENT"] = False
        inserted["CURRENT_FROM_AT"] = None
        inserted["CURRENT_TO_AT"] = None

        if duplicate_of is not None:
            inserted["VERSION_REASON"] = "RE_EXPORT_IDENTICAL"
            inserted["VERSION_STATUS"] = "DUPLICATE"
            inserted["DUPLICATE_OF_CONTENT_VERSION_ID"] = duplicate_of["CONTENT_VERSION_ID"]
            inserted["DUPLICATE_REASON"] = "IDENTICAL_CONTENT_FINGERPRINT"
            inserted["PROMOTE_TO_CURRENT"] = False
        elif row["PROMOTE_TO_CURRENT"]:
            promoted_version_keys_by_item[row["CONTENT_ITEM_KEY"]] = row["CONTENT_VERSION_KEY"]

        state.content_versions[row["CONTENT_VERSION_KEY"]] = inserted

    # Step 4: current-version promotion and pointer update.
    for content_item_key, promoted_version_key in promoted_version_keys_by_item.items():
        promoted_version = state.content_versions[promoted_version_key]
        if promoted_version.get("DUPLICATE_OF_CONTENT_VERSION_ID"):
            continue
        previous_pointer = state.content_item_current_version.get(content_item_key)
        if previous_pointer and previous_pointer["CURRENT_CONTENT_VERSION_KEY"] != promoted_version_key:
            previous_version = state.content_versions[previous_pointer["CURRENT_CONTENT_VERSION_KEY"]]
            previous_version["VERSION_STATUS"] = "SUPERSEDED"
            previous_version["IS_CURRENT"] = False
            previous_version["CURRENT_TO_AT"] = promoted_version["PROPOSED_CURRENT_FROM_AT"] or promoted_version["STAGED_AT"]
            previous_version["UPDATED_AT"] = promoted_version["STAGED_AT"]
            previous_version["SUPERSEDED_BY_CONTENT_VERSION_ID"] = promoted_version["CONTENT_VERSION_ID"]
            promoted_version["PREVIOUS_CONTENT_VERSION_ID"] = previous_version["CONTENT_VERSION_ID"]

        promoted_version["VERSION_STATUS"] = "CURRENT"
        promoted_version["IS_CURRENT"] = True
        promoted_version["CURRENT_FROM_AT"] = promoted_version["PROPOSED_CURRENT_FROM_AT"] or promoted_version["STAGED_AT"]
        promoted_version["CURRENT_TO_AT"] = None
        promoted_version["UPDATED_AT"] = promoted_version["STAGED_AT"]
        state.content_item_current_version[content_item_key] = {
            "CONTENT_ID": promoted_version["CONTENT_ID"],
            "CURRENT_CONTENT_VERSION_ID": promoted_version["CONTENT_VERSION_ID"],
            "CURRENT_CONTENT_VERSION_KEY": promoted_version["CONTENT_VERSION_KEY"],
            "SELECTED_AT": promoted_version["PROPOSED_CURRENT_FROM_AT"] or promoted_version["STAGED_AT"],
            "SELECTION_REASON": promoted_version["CURRENT_SELECTION_REASON"],
            "UPDATED_AT": promoted_version["STAGED_AT"],
        }

    # Steps 5-9: merge child rows by business key.
    _merge_by_key(state.content_bodies, deduped.content_bodies, "CONTENT_BODY_KEY")
    _merge_by_key(state.content_assets, deduped.content_assets, "CONTENT_ASSET_KEY")
    _merge_by_key(state.content_scopes, deduped.content_scopes, "CONTENT_SCOPE_KEY")
    _merge_by_key(state.content_entity_links, deduped.content_entity_links, "CONTENT_ENTITY_LINK_KEY")
    _merge_by_key(state.content_dates, deduped.content_dates, "CONTENT_DATE_KEY")
    return state


def _merge_by_key(target_table: dict[str, StageRow], rows: list[StageRow], business_key: str) -> None:
    for row in rows:
        existing = target_table.get(row[business_key])
        if existing:
            existing.update(row)
        else:
            target_table[row[business_key]] = dict(row)


def _dedupe_identical_stage_rows(batch: StagingBatch) -> StagingBatch:
    return StagingBatch(
        ingestion_run_id=batch.ingestion_run_id,
        content_items=_dedupe_rows(batch.content_items, "CONTENT_ITEM_KEY"),
        content_versions=_dedupe_rows(batch.content_versions, "CONTENT_VERSION_KEY"),
        content_bodies=_dedupe_rows(batch.content_bodies, "CONTENT_BODY_KEY"),
        content_assets=_dedupe_rows(batch.content_assets, "CONTENT_ASSET_KEY"),
        content_scopes=_dedupe_rows(batch.content_scopes, "CONTENT_SCOPE_KEY"),
        content_entity_links=_dedupe_rows(batch.content_entity_links, "CONTENT_ENTITY_LINK_KEY"),
        content_dates=_dedupe_rows(batch.content_dates, "CONTENT_DATE_KEY"),
    )


def _dedupe_rows(rows: list[StageRow], business_key: str) -> list[StageRow]:
    deduped: dict[str, StageRow] = {}
    for row in rows:
        key = row[business_key]
        existing = deduped.get(key)
        if existing and existing["STAGE_PAYLOAD_HASH"] == row["STAGE_PAYLOAD_HASH"]:
            continue
        deduped[key] = dict(row)
    return list(deduped.values())


def _build_body_rows(
    *,
    parsed_document: ParsedDocument,
    content_id: str,
    content_item_key: str,
    body_format: str,
    ingestion_run_id: str,
    staged_at: str,
) -> tuple[list[StageRow], dict[str, StageRow]]:
    rows: list[StageRow] = []
    body_id_to_row: dict[str, StageRow] = {}
    bodies = parsed_document.bodies
    for order, body in enumerate(bodies, start=1):
        normalized_body_hash = make_body_hash(body.text)
        row: StageRow = {
            "INGESTION_RUN_ID": ingestion_run_id,
            "CONTENT_ID": content_id,
            "CONTENT_ITEM_KEY": content_item_key,
            "CONTENT_VERSION_ID": None,
            "CONTENT_VERSION_KEY": None,
            "CONTENT_BODY_ID": body.body_id,
            "CONTENT_BODY_KEY": None,
            "_ORIGINAL_PARENT_BODY_ID": body.parent_body_id,
            "PARENT_CONTENT_BODY_KEY": None,
            "BODY_ROLE": body.body_role,
            "BODY_FORMAT": body_format,
            "BODY_TEXT": body.text,
            "BODY_LOCATION": None,
            "SECTION_PATH": body.section_path,
            "SECTION_TITLE": body.heading,
            "SECTION_LEVEL": body.metadata.get("heading_level"),
            "SECTION_ORDER": order,
            "ANCHOR_START": None,
            "ANCHOR_END": None,
            "IS_PRIMARY_BODY": body.body_role == "PRIMARY",
            "BODY_HASH": normalized_body_hash,
            "NORMALIZED_BODY_HASH": normalized_body_hash,
            "LANGUAGE_CODE": "en",
            "TOKEN_COUNT": len(body.text.split()),
            "EXTRACTION_METHOD": "MARKDOWN_PARSE",
            "EXTRACTION_CONFIDENCE": 1.0,
            "METADATA": body.metadata,
            "STAGED_AT": staged_at,
        }
        rows.append(row)
        body_id_to_row[body.body_id] = row
    return rows, body_id_to_row


def _build_asset_rows(
    *,
    assets: list[AssetCandidate],
    body_id_map: dict[str, StageRow],
    content_id: str,
    content_item_key: str,
    content_version_id: str,
    content_version_key: str,
    ingestion_run_id: str,
    staged_at: str,
) -> tuple[list[StageRow], dict[str | None, StageRow]]:
    rows: list[StageRow] = []
    asset_id_map: dict[str | None, StageRow] = {}
    for order, asset in enumerate(assets, start=1):
        body_row = body_id_map.get(asset.content_body_id)
        content_body_key = body_row["CONTENT_BODY_KEY"] if body_row else None
        content_asset_key = make_content_asset_key(
            content_version_key=content_version_key,
            source_asset_uri=asset.source_asset_uri,
            file_name=asset.file_name or asset.asset_path,
            asset_order=order,
            asset_hash=asset.asset_hash,
        )
        asset_row: StageRow = {
            "INGESTION_RUN_ID": ingestion_run_id,
            "CONTENT_ASSET_ID": make_deterministic_id("content_asset", content_asset_key),
            "CONTENT_ID": content_id,
            "CONTENT_ITEM_KEY": content_item_key,
            "CONTENT_VERSION_ID": content_version_id,
            "CONTENT_VERSION_KEY": content_version_key,
            "CONTENT_BODY_ID": body_row["CONTENT_BODY_ID"] if body_row else None,
            "CONTENT_BODY_KEY": content_body_key,
            "CONTENT_ASSET_KEY": content_asset_key,
            "ASSET_ROLE": _asset_role(asset.asset_type),
            "ASSET_TYPE": asset.asset_type,
            "SOURCE_ASSET_URI": asset.source_asset_uri,
            "STORAGE_URI": asset.asset_path,
            "FILE_NAME": asset.file_name,
            "MIME_TYPE": None,
            "ASSET_HASH": normalize_hash_value(asset.asset_hash),
            "ASSET_ORDER": order,
            "BODY_ANCHOR": asset.anchor_section_path,
            "ALT_TEXT": asset.alt_text,
            "CAPTION_TEXT": asset.caption_text,
            "OCR_TEXT": asset.ocr_text,
            "CHART_DATA": None,
            "EXTRACTION_METHOD": "MARKDOWN_REFERENCE",
            "EXTRACTION_STATUS": _asset_extraction_status(asset.extraction_status),
            "EXTRACTION_CONFIDENCE": 1.0 if asset.extraction_status == "AVAILABLE" else 0.5,
            "METADATA": asset.metadata,
            "STAGED_AT": staged_at,
        }
        asset_row["STAGE_ROW_HASH"], asset_row["STAGE_PAYLOAD_HASH"] = _stage_hashes(asset_row)
        rows.append(asset_row)
        asset_id_map[asset.asset_id] = asset_row
    return rows, asset_id_map


def _build_scope_rows(
    *,
    scopes: list[ScopeCandidate],
    content_id: str,
    content_item_key: str,
    content_version_id: str,
    content_version_key: str,
    ingestion_run_id: str,
    staged_at: str,
) -> list[StageRow]:
    rows: list[StageRow] = []
    for scope in scopes:
        content_scope_key = make_content_scope_key(
            content_version_key=content_version_key,
            scope_type=scope.scope_type,
            scope_entity_id=scope.scope_entity_id,
            scope_entity_name=scope.scope_entity_name,
            scope_label=scope.scope_label,
        )
        row: StageRow = {
            "INGESTION_RUN_ID": ingestion_run_id,
            "CONTENT_SCOPE_ID": make_deterministic_id("content_scope", content_scope_key),
            "CONTENT_ID": content_id,
            "CONTENT_ITEM_KEY": content_item_key,
            "CONTENT_VERSION_ID": content_version_id,
            "CONTENT_VERSION_KEY": content_version_key,
            "CONTENT_SCOPE_KEY": content_scope_key,
            "SCOPE_TYPE": scope.scope_type,
            "SCOPE_LABEL": scope.scope_label,
            "SCOPE_ENTITY_TYPE": scope.scope_entity_type,
            "SCOPE_ENTITY_ID": scope.scope_entity_id,
            "SCOPE_ENTITY_NAME": scope.scope_entity_name,
            "IS_PRIMARY_SCOPE": scope.is_primary_scope,
            "CONFIDENCE": scope.confidence,
            "EVIDENCE_TEXT": scope.evidence_text,
            "EVIDENCE": None,
            "CLASSIFICATION_METHOD": "RULE_BASED",
            "REVIEW_STATUS": scope.review_status,
            "METADATA": scope.metadata,
            "STAGED_AT": staged_at,
        }
        row["STAGE_ROW_HASH"], row["STAGE_PAYLOAD_HASH"] = _stage_hashes(row)
        rows.append(row)
    return rows


def _build_entity_link_rows(
    *,
    entity_links: list[EntityLinkCandidate],
    body_id_map: dict[str, StageRow],
    asset_id_map: dict[str | None, StageRow],
    content_id: str,
    content_item_key: str,
    content_version_id: str,
    content_version_key: str,
    ingestion_run_id: str,
    staged_at: str,
) -> list[StageRow]:
    rows: list[StageRow] = []
    for link in entity_links:
        body_row = body_id_map.get(link.content_body_id)
        asset_row = asset_id_map.get(None) if False else None
        content_body_id = body_row["CONTENT_BODY_ID"] if body_row else None
        content_body_key = body_row["CONTENT_BODY_KEY"] if body_row else None
        content_asset_id = asset_row["CONTENT_ASSET_ID"] if asset_row else None
        content_asset_key = asset_row["CONTENT_ASSET_KEY"] if asset_row else None
        content_entity_link_key = make_content_entity_link_key(
            content_version_key=content_version_key,
            entity_type=link.entity_type,
            entity_id=link.entity_id,
            entity_name=link.entity_name,
            content_body_id=content_body_id,
            content_asset_id=content_asset_id,
            mention_start=link.mention_start,
            mention_end=link.mention_end,
        )
        row: StageRow = {
            "INGESTION_RUN_ID": ingestion_run_id,
            "CONTENT_ENTITY_LINK_ID": make_deterministic_id("content_entity_link", content_entity_link_key),
            "CONTENT_ID": content_id,
            "CONTENT_ITEM_KEY": content_item_key,
            "CONTENT_VERSION_ID": content_version_id,
            "CONTENT_VERSION_KEY": content_version_key,
            "CONTENT_BODY_ID": content_body_id,
            "CONTENT_BODY_KEY": content_body_key,
            "CONTENT_ASSET_ID": content_asset_id,
            "CONTENT_ASSET_KEY": content_asset_key,
            "CONTENT_ENTITY_LINK_KEY": content_entity_link_key,
            "ENTITY_TYPE": link.entity_type,
            "ENTITY_ID": link.entity_id,
            "ENTITY_NAME": link.entity_name,
            "MENTION_TEXT": link.mention_text,
            "MENTION_START": link.mention_start,
            "MENTION_END": link.mention_end,
            "MENTION_CONTEXT": link.mention_context,
            "LINK_ROLE": link.link_role,
            "ANCHOR_SCOPE": link.anchor_scope,
            "RESOLUTION_METHOD": "RULE_BASED_EXACT_MATCH" if link.entity_id else "UNRESOLVED_HEADING",
            "CONFIDENCE": link.confidence,
            "EVIDENCE_TEXT": link.evidence_text,
            "EVIDENCE": None,
            "REVIEW_STATUS": link.review_status,
            "IS_PRIMARY_SUBJECT": link.link_role in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"},
            "METADATA": link.metadata,
            "STAGED_AT": staged_at,
        }
        row["STAGE_ROW_HASH"], row["STAGE_PAYLOAD_HASH"] = _stage_hashes(row)
        rows.append(row)
    return rows


def _build_date_rows(
    *,
    dates: list[DateCandidate],
    body_id_map: dict[str, StageRow],
    content_id: str,
    content_item_key: str,
    content_version_id: str,
    content_version_key: str,
    ingestion_run_id: str,
    staged_at: str,
) -> list[StageRow]:
    rows: list[StageRow] = []
    for candidate in dates:
        body_row = body_id_map.get(candidate.content_body_id) if candidate.content_body_id else None
        content_date_key = make_content_date_key(
            content_version_key=content_version_key,
            date_type=candidate.date_type,
            date_value=candidate.date_value,
            timestamp_value=candidate.timestamp_value,
            period_start_date=candidate.period_start_date,
            period_end_date=candidate.period_end_date,
            date_text=candidate.date_text,
        )
        row: StageRow = {
            "INGESTION_RUN_ID": ingestion_run_id,
            "CONTENT_DATE_ID": make_deterministic_id("content_date", content_date_key),
            "CONTENT_ID": content_id,
            "CONTENT_ITEM_KEY": content_item_key,
            "CONTENT_VERSION_ID": content_version_id,
            "CONTENT_VERSION_KEY": content_version_key,
            "CONTENT_BODY_ID": body_row["CONTENT_BODY_ID"] if body_row else None,
            "CONTENT_BODY_KEY": body_row["CONTENT_BODY_KEY"] if body_row else None,
            "CONTENT_DATE_KEY": content_date_key,
            "DATE_TYPE": candidate.date_type,
            "DATE_ROLE": candidate.date_role,
            "DATE_VALUE": candidate.date_value,
            "TIMESTAMP_VALUE": candidate.timestamp_value,
            "PERIOD_START_DATE": candidate.period_start_date,
            "PERIOD_END_DATE": candidate.period_end_date,
            "DATE_GRANULARITY": _date_granularity(candidate),
            "DATE_TEXT": candidate.date_text,
            "DATE_SOURCE": "MARKDOWN_ADAPTER" if candidate.content_body_id else "SOURCE_METADATA",
            "IS_PRIMARY_CANDIDATE": candidate.is_primary_candidate,
            "DATE_PRIORITY_OVERRIDE": None,
            "CONFIDENCE": candidate.confidence,
            "EVIDENCE_TEXT": candidate.evidence_text,
            "EVIDENCE": None,
            "INFERENCE_METHOD": "RULE_BASED_PARSE",
            "REVIEW_STATUS": "UNREVIEWED",
            "METADATA": candidate.metadata,
            "STAGED_AT": staged_at,
        }
        row["STAGE_ROW_HASH"], row["STAGE_PAYLOAD_HASH"] = _stage_hashes(row)
        rows.append(row)
    return rows


def _detect_conflicting_duplicates(table_name: str, rows: list[StageRow], business_key: str) -> list[StageValidationIssue]:
    grouped: dict[str, set[str]] = {}
    issues: list[StageValidationIssue] = []
    for row in rows:
        grouped.setdefault(row[business_key], set()).add(row["STAGE_PAYLOAD_HASH"])
    for key, payload_hashes in grouped.items():
        if len(payload_hashes) > 1:
            issues.append(
                StageValidationIssue(
                    issue_code=f"{table_name}_KEY_CONFLICT",
                    table_name=table_name,
                    business_key=key,
                    detail=f"{business_key} has conflicting staged payloads in the same ingestion run.",
                    context={"payload_hash_count": len(payload_hashes)},
                )
            )
    return issues


def _validate_primary_body(batch: StagingBatch) -> list[StageValidationIssue]:
    counts: dict[str, int] = {}
    for row in batch.content_bodies:
        if row["IS_PRIMARY_BODY"]:
            counts[row["CONTENT_VERSION_KEY"]] = counts.get(row["CONTENT_VERSION_KEY"], 0) + 1
    issues: list[StageValidationIssue] = []
    version_keys = {row["CONTENT_VERSION_KEY"] for row in batch.content_versions}
    for version_key in version_keys:
        primary_count = counts.get(version_key, 0)
        if primary_count != 1:
            issues.append(
                StageValidationIssue(
                    issue_code="STAGE_PRIMARY_BODY_CONFLICT",
                    table_name="STAGE_CONTENT_BODIES",
                    business_key=version_key,
                    detail="Each staged content version must have exactly one primary body.",
                    context={"primary_body_count": primary_count},
                )
            )
    return issues


def _validate_current_version_candidates(batch: StagingBatch) -> list[StageValidationIssue]:
    promoted_by_item: dict[str, set[str]] = {}
    for row in batch.content_versions:
        if row["PROMOTE_TO_CURRENT"]:
            promoted_by_item.setdefault(row["CONTENT_ITEM_KEY"], set()).add(row["CONTENT_VERSION_KEY"])
    issues: list[StageValidationIssue] = []
    for content_item_key, version_keys in promoted_by_item.items():
        if len(version_keys) > 1:
            issues.append(
                StageValidationIssue(
                    issue_code="STAGE_MULTIPLE_CURRENT_VERSION_CANDIDATES",
                    table_name="STAGE_CONTENT_VERSIONS",
                    business_key=content_item_key,
                    detail="A single batch cannot promote more than one current version for the same content item.",
                    context={"candidate_version_keys": sorted(version_keys)},
                )
            )
    return issues


def _validate_parent_references(batch: StagingBatch) -> list[StageValidationIssue]:
    issues: list[StageValidationIssue] = []
    body_keys = {row["CONTENT_BODY_KEY"] for row in batch.content_bodies}
    asset_keys = {row["CONTENT_ASSET_KEY"] for row in batch.content_assets}
    for row in batch.content_assets:
        parent_key = row.get("CONTENT_BODY_KEY")
        if parent_key and parent_key not in body_keys:
            issues.append(
                StageValidationIssue(
                    issue_code="STAGE_ORPHAN_ASSET_BODY_REFERENCE",
                    table_name="STAGE_CONTENT_ASSETS",
                    business_key=row["CONTENT_ASSET_KEY"],
                    detail="Asset references a body key that is not staged in this batch.",
                    context={"content_body_key": parent_key},
                )
            )
    for row in batch.content_entity_links:
        body_key = row.get("CONTENT_BODY_KEY")
        asset_key = row.get("CONTENT_ASSET_KEY")
        if body_key and body_key not in body_keys:
            issues.append(
                StageValidationIssue(
                    issue_code="STAGE_ORPHAN_ENTITY_BODY_REFERENCE",
                    table_name="STAGE_CONTENT_ENTITY_LINKS",
                    business_key=row["CONTENT_ENTITY_LINK_KEY"],
                    detail="Entity link references a body key that is not staged in this batch.",
                    context={"content_body_key": body_key},
                )
            )
        if asset_key and asset_key not in asset_keys:
            issues.append(
                StageValidationIssue(
                    issue_code="STAGE_ORPHAN_ENTITY_ASSET_REFERENCE",
                    table_name="STAGE_CONTENT_ENTITY_LINKS",
                    business_key=row["CONTENT_ENTITY_LINK_KEY"],
                    detail="Entity link references an asset key that is not staged in this batch.",
                    context={"content_asset_key": asset_key},
                )
            )
    for row in batch.content_dates:
        body_key = row.get("CONTENT_BODY_KEY")
        if body_key and body_key not in body_keys:
            issues.append(
                StageValidationIssue(
                    issue_code="STAGE_ORPHAN_DATE_BODY_REFERENCE",
                    table_name="STAGE_CONTENT_DATES",
                    business_key=row["CONTENT_DATE_KEY"],
                    detail="Date candidate references a body key that is not staged in this batch.",
                    context={"content_body_key": body_key},
                )
            )
    return issues


def _stage_hashes(row: StageRow) -> tuple[str, str]:
    payload = {key: value for key, value in row.items() if key not in {"STAGE_ROW_HASH", "STAGE_PAYLOAD_HASH"}}
    payload_hash = make_deterministic_id("payload", canonical_json(payload), length=32)
    row_hash = make_deterministic_id("row", canonical_json(payload), length=32)
    return row_hash, payload_hash


def _asset_role(asset_type: str | None) -> str:
    normalized = (asset_type or "").upper()
    if normalized in {"IMAGE", "SCREENSHOT", "TABLE_IMAGE"}:
        return "EMBEDDED_IMAGE"
    if normalized == "CHART":
        return "CHART"
    return "ATTACHMENT"


def _asset_extraction_status(status: str) -> str:
    if status == "AVAILABLE":
        return "EXTRACTED"
    if status == "MISSING_FILE":
        return "FAILED"
    return "NOT_APPLICABLE"


def _date_granularity(candidate: DateCandidate) -> str:
    if candidate.period_start_date or candidate.period_end_date:
        return "PERIOD"
    if candidate.timestamp_value:
        return "TIMESTAMP"
    return "DATE"


def _default_version_reason(*, bundle: ResolvedBundle, promote_to_current: bool) -> str:
    if bundle.source.get("source_export_id"):
        return "SOURCE_EXPORT"
    if promote_to_current:
        return "INITIAL_INGEST"
    return "ADAPTER_REPROCESS"


def _coalesce_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_now_isoformat() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

