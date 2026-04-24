"""Read markdown-note bundles from the pilot manifest contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .normalize import blank_to_none


class UnreadableBundleError(ValueError):
    """Raised when the markdown bundle cannot be read as text."""


@dataclass(frozen=True)
class ResolvedAssetInput:
    asset_id: str | None
    asset_path: str
    resolved_path: Path
    file_name: str | None
    source_asset_uri: str | None
    asset_type: str | None
    asset_hash: str | None
    ocr_text: str | None
    caption_text: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedBundle:
    bundle_id: str
    base_dir: Path
    source: dict[str, Any]
    content: dict[str, Any]
    markdown_path: str
    markdown_resolved_path: Path
    markdown_text: str
    title: str
    assets: list[ResolvedAssetInput]
    ingested_at: str | None = None


def read_bundle(bundle: dict[str, Any], base_dir: str | Path, *, ingested_at: str | None = None) -> ResolvedBundle:
    base_path = Path(base_dir)
    bundle_id = _require_text(bundle.get("bundle_id"), "bundle.bundle_id")
    source = _require_mapping(bundle.get("source"), "bundle.source")
    content = _require_mapping(bundle.get("content"), "bundle.content")

    markdown_path = _require_text(content.get("markdown_path"), "bundle.content.markdown_path")
    markdown_resolved_path = (base_path / markdown_path).resolve()
    try:
        markdown_text = markdown_resolved_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise UnreadableBundleError(f"markdown file does not exist: {markdown_path}") from exc
    except UnicodeDecodeError as exc:
        raise UnreadableBundleError(f"markdown file is not valid UTF-8 text: {markdown_path}") from exc

    title = (
        blank_to_none(source.get("source_title"))
        or markdown_resolved_path.stem
        or bundle_id
    )

    assets = [_resolve_asset(asset, base_path) for asset in content.get("assets", []) or []]
    return ResolvedBundle(
        bundle_id=bundle_id,
        base_dir=base_path,
        source=source,
        content=content,
        markdown_path=markdown_path,
        markdown_resolved_path=markdown_resolved_path,
        markdown_text=markdown_text,
        title=title,
        assets=assets,
        ingested_at=ingested_at,
    )


def _resolve_asset(asset: dict[str, Any], base_path: Path) -> ResolvedAssetInput:
    asset_path = _require_text(asset.get("asset_path"), "bundle.content.assets[].asset_path")
    resolved_path = (base_path / asset_path).resolve()
    ocr_text = _read_optional_text(base_path, asset.get("ocr_text_path"))
    caption_text = _read_optional_text(base_path, asset.get("caption_text_path"))
    return ResolvedAssetInput(
        asset_id=blank_to_none(asset.get("asset_id")),
        asset_path=asset_path,
        resolved_path=resolved_path,
        file_name=blank_to_none(asset.get("file_name")) or resolved_path.name,
        source_asset_uri=blank_to_none(asset.get("source_asset_uri")),
        asset_type=blank_to_none(asset.get("asset_type")),
        asset_hash=blank_to_none(asset.get("asset_hash")),
        ocr_text=ocr_text,
        caption_text=caption_text,
        metadata=dict(asset.get("metadata") or {}),
    )


def _read_optional_text(base_path: Path, relative_path: Any) -> str | None:
    text_path = blank_to_none(relative_path)
    if text_path is None:
        return None

    resolved_path = (base_path / text_path).resolve()
    if not resolved_path.exists():
        return None
    try:
        return resolved_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise UnreadableBundleError(f"{field_name} must be an object")
    return value


def _require_text(value: Any, field_name: str) -> str:
    text = blank_to_none(value)
    if text is None:
        raise UnreadableBundleError(f"{field_name} is required")
    return text

