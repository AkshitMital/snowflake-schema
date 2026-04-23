"""Manifest validation for the APS markdown-note pilot contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .normalize import blank_to_none


class ManifestValidationError(ValueError):
    """Raised when a pilot manifest violates the Checkpoint A contract."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("Manifest validation failed: " + "; ".join(errors))


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    errors: list[str] = []

    if not isinstance(manifest, dict):
        raise ManifestValidationError(["manifest must be a JSON object"])

    if manifest.get("manifest_version") != "1.0":
        errors.append("manifest_version must be '1.0'")

    pilot = manifest.get("pilot")
    if not isinstance(pilot, dict):
        errors.append("pilot must be an object")
    elif not blank_to_none(pilot.get("name")):
        errors.append("pilot.name is required")

    bundles = manifest.get("bundles")
    if not isinstance(bundles, list) or not bundles:
        errors.append("bundles must be a non-empty array")
    else:
        seen_bundle_ids: set[str] = set()
        for index, bundle in enumerate(bundles):
            path = f"bundles[{index}]"
            if not isinstance(bundle, dict):
                errors.append(f"{path} must be an object")
                continue

            bundle_id = blank_to_none(bundle.get("bundle_id"))
            if bundle_id is None:
                errors.append(f"{path}.bundle_id is required")
            elif bundle_id in seen_bundle_ids:
                errors.append(f"{path}.bundle_id '{bundle_id}' is duplicated")
            else:
                seen_bundle_ids.add(bundle_id)

            _validate_source(bundle.get("source"), path, errors)
            _validate_content(bundle.get("content"), path, errors)

            labels = bundle.get("expected", {}).get("edge_case_labels", [])
            if labels is not None and not isinstance(labels, list):
                errors.append(f"{path}.expected.edge_case_labels must be an array when present")

    if errors:
        raise ManifestValidationError(errors)


def _validate_source(source: Any, path: str, errors: list[str]) -> None:
    source_path = f"{path}.source"
    if not isinstance(source, dict):
        errors.append(f"{source_path} must be an object")
        return

    if not blank_to_none(source.get("source_system")):
        errors.append(f"{source_path}.source_system is required")

    if not blank_to_none(source.get("source_type")):
        errors.append(f"{source_path}.source_type is required")

    has_object_id = bool(blank_to_none(source.get("source_object_id")))
    has_uri = bool(blank_to_none(source.get("source_uri")))
    has_container_path = bool(blank_to_none(source.get("source_container")) and blank_to_none(source.get("source_path")))
    if not (has_object_id or has_uri or has_container_path):
        errors.append(
            f"{source_path} requires source_object_id, source_uri, or both source_container and source_path"
        )


def _validate_content(content: Any, path: str, errors: list[str]) -> None:
    content_path = f"{path}.content"
    if not isinstance(content, dict):
        errors.append(f"{content_path} must be an object")
        return

    if not blank_to_none(content.get("markdown_path")):
        errors.append(f"{content_path}.markdown_path is required")

    assets = content.get("assets", [])
    if assets is not None and not isinstance(assets, list):
        errors.append(f"{content_path}.assets must be an array when present")
        return

    for asset_index, asset in enumerate(assets or []):
        asset_path = f"{content_path}.assets[{asset_index}]"
        if not isinstance(asset, dict):
            errors.append(f"{asset_path} must be an object")
            continue
        if not blank_to_none(asset.get("asset_path")):
            errors.append(f"{asset_path}.asset_path is required")

