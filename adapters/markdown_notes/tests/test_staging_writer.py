from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.asset_extractor import extract_assets
from aps_markdown_notes.bundle_reader import read_bundle
from aps_markdown_notes.date_extractor import extract_date_candidates
from aps_markdown_notes.entity_linker import link_entities
from aps_markdown_notes.markdown_parser import parse_bundle
from aps_markdown_notes.scope_classifier import classify_scopes
from aps_markdown_notes.staging_writer import (
    StageValidationError,
    build_staging_batch,
    require_valid_staging_batch,
    simulate_merge_batch,
)


class StagingWriterTests(unittest.TestCase):
    def test_idempotent_rerun(self) -> None:
        batch = self._build_batch(
            markdown="# Acme Solar Update\n\nAcme Solar continues to perform.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-acme-update",
                "source_type": "NOTE",
                "source_title": "Acme Solar Update",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "a" * 64},
            entity_catalog={"INVESTMENT": {"Acme Solar": "inv_acme_solar"}},
            ingestion_run_id="run-001",
        )

        state = simulate_merge_batch(None, batch)
        rerun_batch = self._build_batch(
            markdown="# Acme Solar Update\n\nAcme Solar continues to perform.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-acme-update",
                "source_type": "NOTE",
                "source_title": "Acme Solar Update",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "a" * 64},
            entity_catalog={"INVESTMENT": {"Acme Solar": "inv_acme_solar"}},
            ingestion_run_id="run-002",
        )
        rerun_state = simulate_merge_batch(state, rerun_batch)

        self.assertEqual(state.counts(), rerun_state.counts())
        self.assertEqual(len(rerun_state.content_versions), 1)
        version = next(iter(rerun_state.content_versions.values()))
        self.assertTrue(version["IS_CURRENT"])

    def test_revised_note_creates_new_version(self) -> None:
        initial = self._build_batch(
            markdown="# Acme Solar Update\n\nAcme Solar continues to perform.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-acme-update",
                "source_type": "NOTE",
                "source_title": "Acme Solar Update",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "a" * 64},
            entity_catalog={"INVESTMENT": {"Acme Solar": "inv_acme_solar"}},
            ingestion_run_id="run-001",
        )
        revised = self._build_batch(
            markdown="# Acme Solar Update\n\nAcme Solar accelerated bookings materially.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-acme-update",
                "source_type": "NOTE",
                "source_title": "Acme Solar Update",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "b" * 64},
            entity_catalog={"INVESTMENT": {"Acme Solar": "inv_acme_solar"}},
            ingestion_run_id="run-002",
            version_reason="SOURCE_REVISION",
        )

        state = simulate_merge_batch(None, initial)
        state = simulate_merge_batch(state, revised)

        self.assertEqual(len(state.content_items), 1)
        self.assertEqual(len(state.content_versions), 2)
        current_pointer = next(iter(state.content_item_current_version.values()))
        current_version = state.content_versions[current_pointer["CURRENT_CONTENT_VERSION_KEY"]]
        superseded = [version for version in state.content_versions.values() if version["VERSION_STATUS"] == "SUPERSEDED"]

        self.assertEqual(current_version["VERSION_REASON"], "SOURCE_REVISION")
        self.assertEqual(len(superseded), 1)
        self.assertFalse(superseded[0]["IS_CURRENT"])

    def test_reexport_same_fingerprint_does_not_create_new_current_version(self) -> None:
        initial = self._build_batch(
            markdown="# SAIF Portfolio Redemption\n\nSAIF saw liquidity improve.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-saif-redemption",
                "source_type": "NOTE",
                "source_title": "SAIF Portfolio Redemption",
                "source_version_id": "export-v1",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "c" * 64},
            entity_catalog={"PORTFOLIO": {"SAIF": "portfolio_saif"}},
            ingestion_run_id="run-001",
        )
        reexport = self._build_batch(
            markdown="# SAIF Portfolio Redemption\n\nSAIF saw liquidity improve.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-saif-redemption",
                "source_type": "NOTE",
                "source_title": "SAIF Portfolio Redemption",
                "source_version_id": "export-v2",
                "source_export_id": "sharepoint-export-2",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "c" * 64},
            entity_catalog={"PORTFOLIO": {"SAIF": "portfolio_saif"}},
            ingestion_run_id="run-002",
        )

        state = simulate_merge_batch(None, initial)
        state = simulate_merge_batch(state, reexport)

        self.assertEqual(len(state.content_versions), 2)
        current_pointer = next(iter(state.content_item_current_version.values()))
        current_version = state.content_versions[current_pointer["CURRENT_CONTENT_VERSION_KEY"]]
        duplicate_versions = [version for version in state.content_versions.values() if version["VERSION_STATUS"] == "DUPLICATE"]

        self.assertEqual(current_version["SOURCE_VERSION_ID"], "export-v1")
        self.assertEqual(len(duplicate_versions), 1)
        self.assertFalse(duplicate_versions[0]["IS_CURRENT"])

    def test_moved_file_with_stable_object_id_updates_item_without_new_version(self) -> None:
        original = self._build_batch(
            markdown="# PEP Weekly\n\nPEP Weekly focused on rates.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-pep-weekly",
                "source_type": "NOTE",
                "source_title": "PEP Weekly",
                "source_path": "Old Folder/PEP Weekly.md",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "d" * 64},
            entity_catalog={"STRATEGY": {"PEP Weekly": "strategy_pep_weekly"}},
            ingestion_run_id="run-001",
        )
        moved = self._build_batch(
            markdown="# PEP Weekly\n\nPEP Weekly focused on rates.\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-pep-weekly",
                "source_type": "NOTE",
                "source_title": "PEP Weekly",
                "source_path": "New Folder/Archive/PEP Weekly.md",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "d" * 64},
            entity_catalog={"STRATEGY": {"PEP Weekly": "strategy_pep_weekly"}},
            ingestion_run_id="run-002",
        )

        state = simulate_merge_batch(None, original)
        state = simulate_merge_batch(state, moved)

        self.assertEqual(len(state.content_items), 1)
        self.assertEqual(len(state.content_versions), 1)
        item = next(iter(state.content_items.values()))
        self.assertEqual(item["SOURCE_PATH"], "New Folder/Archive/PEP Weekly.md")

    def test_duplicate_key_conflict_detection(self) -> None:
        batch = self._build_batch(
            markdown="# Strategy Note\n\nMeeting Date: January 15, 2026\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-strategy-note",
                "source_type": "NOTE",
                "source_title": "Strategy Note",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "e" * 64},
            entity_catalog={},
            ingestion_run_id="run-001",
        )
        duplicate = copy.deepcopy(batch.content_items[0])
        duplicate["SOURCE_TITLE"] = "Conflicting Title"
        duplicate["STAGE_ROW_HASH"] = "ROW_conflict"
        duplicate["STAGE_PAYLOAD_HASH"] = "PAYLOAD_conflict"
        batch.content_items.append(duplicate)

        with self.assertRaises(StageValidationError) as error:
            require_valid_staging_batch(batch)

        self.assertIn("STAGE_CONTENT_ITEMS_KEY_CONFLICT", str(error.exception))

    def test_multiple_current_version_conflict_blocking(self) -> None:
        batch = self._build_batch(
            markdown="# Strategy Note\n\nMeeting Date: January 15, 2026\n",
            source={
                "source_system": "SharePoint Online",
                "source_object_id": "sp-strategy-note",
                "source_type": "NOTE",
                "source_title": "Strategy Note",
            },
            content={"markdown_path": "bundles/note.md", "source_file_hash": "f" * 64},
            entity_catalog={},
            ingestion_run_id="run-001",
        )
        conflicting_version = copy.deepcopy(batch.content_versions[0])
        conflicting_version["CONTENT_VERSION_KEY"] = conflicting_version["CONTENT_VERSION_KEY"] + ":alt"
        conflicting_version["CONTENT_VERSION_ID"] = conflicting_version["CONTENT_VERSION_ID"] + "_ALT"
        conflicting_version["BODY_HASH"] = "deadbeef" * 8
        conflicting_version["CONTENT_FINGERPRINT"] = "feedface" * 8
        conflicting_version["STAGE_ROW_HASH"] = "ROW_multiple_current"
        conflicting_version["STAGE_PAYLOAD_HASH"] = "PAYLOAD_multiple_current"
        batch.content_versions.append(conflicting_version)

        with self.assertRaises(StageValidationError) as error:
            simulate_merge_batch(None, batch)

        self.assertIn("STAGE_MULTIPLE_CURRENT_VERSION_CANDIDATES", str(error.exception))

    def _build_batch(
        self,
        *,
        markdown: str,
        source: dict,
        content: dict,
        entity_catalog: dict[str, dict[str, str]],
        ingestion_run_id: str,
        version_reason: str | None = None,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            note_path = temp_path / content["markdown_path"]
            note_path.parent.mkdir(parents=True, exist_ok=True)
            note_path.write_text(markdown, encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": source.get("source_object_id") or "bundle-note",
                    "source": source,
                    "content": content,
                },
                temp_path,
                ingested_at="2026-04-24T12:00:00Z",
            )
            parsed = parse_bundle(bundle)
            assets = extract_assets(bundle, parsed)
            entity_links = link_entities(parsed, entity_catalog=entity_catalog)
            scopes = classify_scopes(parsed, entity_links)
            dates = extract_date_candidates(bundle, parsed)
            return build_staging_batch(
                bundle=bundle,
                parsed_document=parsed,
                assets=assets,
                scopes=scopes,
                entity_links=entity_links,
                dates=dates,
                ingestion_run_id=ingestion_run_id,
                adapter_version="0.1.0-test",
                version_reason=version_reason,
            )


if __name__ == "__main__":
    unittest.main()

