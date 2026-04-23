from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.manifest import ManifestValidationError, load_manifest, validate_manifest


FIXTURES = ROOT / "fixtures"


class ManifestContractTests(unittest.TestCase):
    def test_example_manifest_validates(self) -> None:
        manifest = load_manifest(FIXTURES / "pilot_manifest.example.json")

        self.assertEqual(manifest["manifest_version"], "1.0")
        self.assertGreaterEqual(len(manifest["bundles"]), 5)

    def test_schema_file_is_valid_json_and_declares_required_contract(self) -> None:
        schema = json.loads((ROOT / "manifest.schema.json").read_text(encoding="utf-8"))

        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertIn("bundles", schema["required"])
        self.assertIn("source", schema["$defs"])
        self.assertIn("content", schema["$defs"])

    def test_container_path_identity_fallback_validates_without_source_object_id(self) -> None:
        manifest = self._minimal_manifest()
        del manifest["bundles"][0]["source"]["source_object_id"]
        manifest["bundles"][0]["source"]["source_container"] = "APS Notes"
        manifest["bundles"][0]["source"]["source_path"] = "Strategy/Undated.md"

        validate_manifest(manifest)

    def test_undated_note_validates_without_source_dates(self) -> None:
        manifest = self._minimal_manifest()
        source = manifest["bundles"][0]["source"]
        source.pop("source_created_at", None)
        source.pop("source_modified_at", None)
        source.pop("source_exported_at", None)

        validate_manifest(manifest)

    def test_image_heavy_note_allows_partial_ocr_context(self) -> None:
        manifest = self._minimal_manifest()
        manifest["bundles"][0]["content"]["assets"] = [
            {
                "asset_path": "bundle/assets/slide-001.png",
                "asset_type": "SCREENSHOT",
                "ocr_text_path": "bundle/assets/slide-001.ocr.txt",
            },
            {
                "asset_path": "bundle/assets/slide-002.png",
                "asset_type": "SCREENSHOT",
            },
        ]

        validate_manifest(manifest)

    def test_missing_markdown_path_fails_validation(self) -> None:
        manifest = self._minimal_manifest()
        del manifest["bundles"][0]["content"]["markdown_path"]

        with self.assertRaises(ManifestValidationError) as error:
            validate_manifest(manifest)

        self.assertIn("markdown_path", str(error.exception))

    def test_missing_source_identity_fallback_fails_validation(self) -> None:
        manifest = self._minimal_manifest()
        source = manifest["bundles"][0]["source"]
        source.pop("source_object_id", None)
        source.pop("source_uri", None)
        source.pop("source_container", None)
        source.pop("source_path", None)

        with self.assertRaises(ManifestValidationError) as error:
            validate_manifest(manifest)

        self.assertIn("source_object_id", str(error.exception))

    def test_duplicate_bundle_ids_fail_validation(self) -> None:
        manifest = self._minimal_manifest()
        duplicate = copy.deepcopy(manifest["bundles"][0])
        manifest["bundles"].append(duplicate)

        with self.assertRaises(ManifestValidationError) as error:
            validate_manifest(manifest)

        self.assertIn("duplicated", str(error.exception))

    def _minimal_manifest(self) -> dict:
        return {
            "manifest_version": "1.0",
            "pilot": {"name": "Unit test pilot"},
            "bundles": [
                {
                    "bundle_id": "unit-test-note",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-unit-test-note",
                        "source_type": "NOTE",
                        "source_title": "Unit Test Note",
                    },
                    "content": {
                        "markdown_path": "bundles/unit-test-note/note.md",
                    },
                }
            ],
        }


if __name__ == "__main__":
    unittest.main()

