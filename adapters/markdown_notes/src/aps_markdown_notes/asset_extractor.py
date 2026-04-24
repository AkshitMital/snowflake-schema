"""Extract image and attachment candidates from parsed markdown bundles."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any

from .bundle_reader import ResolvedAssetInput, ResolvedBundle
from .keys import make_deterministic_id
from .markdown_parser import ParsedDocument
from .normalize import blank_to_none, normalize_source_path, normalize_source_uri

_IMAGE_RE = re.compile(r"!\[(?P<label>[^\]]*)\]\((?P<target>[^)]+)\)")
_LINK_RE = re.compile(r"(?<!\!)\[(?P<label>[^\]]+)\]\((?P<target>[^)]+)\)")
_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".svg"}


@dataclass(frozen=True)
class AssetCandidate:
    asset_id: str
    asset_path: str
    file_name: str | None
    source_asset_uri: str | None
    asset_type: str
    asset_hash: str | None
    content_body_id: str
    anchor_section_path: str | None
    alt_text: str | None
    ocr_text: str | None
    caption_text: str | None
    extraction_status: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _AssetReference:
    raw_target: str
    normalized_target: str | None
    alt_text: str | None
    content_body_id: str
    anchor_section_path: str | None
    reference_kind: str
    order: int


def extract_assets(bundle: ResolvedBundle, parsed_document: ParsedDocument) -> list[AssetCandidate]:
    reference_bodies = parsed_document.section_bodies or [parsed_document.primary_body]
    references = _extract_markdown_references(reference_bodies)

    manifest_assets = [_build_manifest_asset(asset, parsed_document, references) for asset in bundle.assets]
    consumed_targets: set[str] = set()
    for candidate in manifest_assets:
        consumed_targets.update(_reference_lookup_keys(candidate.asset_path, candidate.file_name))

    inferred_assets: list[AssetCandidate] = []
    for reference in references:
        lookup_keys = _reference_lookup_keys(reference.normalized_target, Path(reference.raw_target).name)
        if consumed_targets.intersection(lookup_keys):
            continue
        inferred_assets.append(
            AssetCandidate(
                asset_id=make_deterministic_id(
                    "content_asset",
                    f"{parsed_document.document_id}:{reference.order}:{reference.raw_target}",
                ),
                asset_path=reference.raw_target,
                file_name=Path(reference.raw_target).name if reference.raw_target else None,
                source_asset_uri=reference.raw_target if "://" in reference.raw_target else None,
                asset_type=_infer_asset_type(reference.raw_target),
                asset_hash=None,
                content_body_id=reference.content_body_id,
                anchor_section_path=reference.anchor_section_path,
                alt_text=reference.alt_text,
                ocr_text=None,
                caption_text=None,
                extraction_status="REFERENCED_ONLY",
                metadata={"reference_kind": reference.reference_kind},
            )
        )

    return [*manifest_assets, *inferred_assets]


def _extract_markdown_references(reference_bodies: list[Any]) -> list[_AssetReference]:
    references: list[_AssetReference] = []
    order = 0
    for body in reference_bodies:
        for pattern, reference_kind in ((_IMAGE_RE, "IMAGE"), (_LINK_RE, "LINK")):
            for match in pattern.finditer(body.text):
                raw_target = _clean_markdown_target(match.group("target"))
                if not _looks_like_asset(raw_target):
                    continue
                order += 1
                references.append(
                    _AssetReference(
                        raw_target=raw_target,
                        normalized_target=_normalize_target(raw_target),
                        alt_text=blank_to_none(match.group("label")),
                        content_body_id=body.body_id,
                        anchor_section_path=body.section_path,
                        reference_kind=reference_kind,
                        order=order,
                    )
                )
    return references


def _build_manifest_asset(
    asset: ResolvedAssetInput,
    parsed_document: ParsedDocument,
    references: list[_AssetReference],
) -> AssetCandidate:
    normalized_manifest_path = normalize_source_path(asset.asset_path)
    normalized_file_name = normalize_source_path(asset.file_name)
    matching_reference = _find_matching_reference(references, normalized_manifest_path, normalized_file_name)
    anchor_body_id = matching_reference.content_body_id if matching_reference else parsed_document.primary_body.body_id
    anchor_section_path = matching_reference.anchor_section_path if matching_reference else parsed_document.primary_body.section_path
    extraction_status = "AVAILABLE" if asset.resolved_path.exists() else "MISSING_FILE"
    return AssetCandidate(
        asset_id=asset.asset_id
        or make_deterministic_id(
            "content_asset",
            f"{parsed_document.document_id}:{normalized_manifest_path or asset.asset_path}",
        ),
        asset_path=asset.asset_path,
        file_name=asset.file_name,
        source_asset_uri=asset.source_asset_uri,
        asset_type=blank_to_none(asset.asset_type) or _infer_asset_type(asset.file_name or asset.asset_path),
        asset_hash=asset.asset_hash,
        content_body_id=anchor_body_id,
        anchor_section_path=anchor_section_path,
        alt_text=matching_reference.alt_text if matching_reference else None,
        ocr_text=asset.ocr_text,
        caption_text=asset.caption_text,
        extraction_status=extraction_status,
        metadata={"reference_kind": matching_reference.reference_kind if matching_reference else "MANIFEST"},
    )


def _find_matching_reference(
    references: list[_AssetReference],
    normalized_manifest_path: str | None,
    normalized_file_name: str | None,
) -> _AssetReference | None:
    for reference in references:
        if normalized_manifest_path and reference.normalized_target == normalized_manifest_path:
            return reference
        if normalized_file_name and Path(reference.raw_target).name.lower() == Path(normalized_file_name).name.lower():
            return reference
    return None


def _clean_markdown_target(target: str) -> str:
    target = target.strip()
    if " " in target and "\"" in target:
        target = target.split(" ", 1)[0]
    return target.strip("<>")


def _normalize_target(target: str) -> str | None:
    if "://" in target:
        return normalize_source_uri(target)
    return normalize_source_path(target)


def _looks_like_asset(target: str) -> bool:
    suffix = Path(target).suffix.lower()
    return suffix in _IMAGE_SUFFIXES or suffix in {".pdf", ".pptx", ".xlsx", ".csv"}


def _infer_asset_type(path_like: str | None) -> str:
    suffix = Path(path_like or "").suffix.lower()
    if suffix in _IMAGE_SUFFIXES:
        return "IMAGE"
    return "ATTACHMENT"


def _reference_lookup_keys(asset_path: str | None, file_name: str | None) -> set[str]:
    normalized_path = normalize_source_path(asset_path)
    normalized_name = normalize_source_path(file_name)
    return {value for value in (normalized_path, normalized_name) if value}
