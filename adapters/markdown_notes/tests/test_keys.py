from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.keys import (
    KeyGenerationError,
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


FIXTURES = ROOT / "fixtures"


class KeyGenerationTests(unittest.TestCase):
    def test_deterministic_key_fixtures_match_expected_outputs(self) -> None:
        fixtures = json.loads((FIXTURES / "deterministic_key_fixtures.json").read_text(encoding="utf-8"))

        for fixture in fixtures:
            with self.subTest(fixture=fixture["name"]):
                item_input = {
                    key: value
                    for key, value in fixture["input"].items()
                    if key != "content_fingerprint"
                }
                item_key = make_content_item_key(**item_input)
                version_key = make_content_version_key(
                    content_item_key=item_key,
                    content_fingerprint=fixture["input"]["content_fingerprint"],
                )

                self.assertEqual(item_key, fixture["expected"]["content_item_key"])
                self.assertEqual(version_key, fixture["expected"]["content_version_key"])

                # Rerun stability: same inputs produce byte-identical keys.
                self.assertEqual(make_content_item_key(**item_input), item_key)
                self.assertEqual(
                    make_content_version_key(
                        content_item_key=item_key,
                        content_fingerprint=fixture["input"]["content_fingerprint"],
                    ),
                    version_key,
                )

    def test_moved_file_with_stable_source_object_id_keeps_item_key(self) -> None:
        original = {
            "source_system": "SharePoint Online",
            "source_object_id": "sp-note-moved-001",
            "source_path": "Old Folder/SAIF Portfolio Redemption.md",
        }
        moved = {
            "source_system": "SharePoint Online",
            "source_object_id": "sp-note-moved-001",
            "source_path": "New Folder/Archive/SAIF Portfolio Redemption.md",
        }

        self.assertEqual(make_content_item_key(**original), make_content_item_key(**moved))

    def test_same_content_with_same_fingerprint_keeps_version_key(self) -> None:
        item_key = make_content_item_key(
            source_system="SharePoint Online",
            source_object_id="sp-note-same-content-001",
        )
        first = make_content_version_key(
            content_item_key=item_key,
            content_fingerprint="CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        )
        second = make_content_version_key(
            content_item_key=item_key,
            content_fingerprint="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        )

        self.assertEqual(first, second)

    def test_whitespace_and_path_normalization_prevent_key_drift(self) -> None:
        left = make_content_item_key(
            source_system=" SharePoint   Online ",
            source_container=" APS Notes ",
            source_path="\\Strategy//PEP%20Weekly.md ",
        )
        right = make_content_item_key(
            source_system="sharepoint-online",
            source_container="aps notes",
            source_path="strategy/pep weekly.md",
        )

        self.assertEqual(left, right)
        self.assertEqual(left, "SHAREPOINT_ONLINE:aps notes/strategy/pep weekly.md")

    def test_item_identity_records_fallback_method(self) -> None:
        identity = resolve_item_identity(
            source_system="Local Markdown Export",
            source_container="APS Notes",
            source_path="Strategy/Undated Note.md",
        )

        self.assertEqual(identity.item_identity_method, "CONTAINER_PATH")
        self.assertEqual(identity.content_item_key, "LOCAL_MARKDOWN_EXPORT:aps notes/strategy/undated note.md")

    def test_missing_item_identity_fails(self) -> None:
        with self.assertRaises(KeyGenerationError):
            make_content_item_key(source_system="SharePoint Online")

    def test_child_key_formulas_match_contract(self) -> None:
        version_key = "SHAREPOINT_ONLINE:sp-note-001:aaaaaaaa"
        body_key = make_content_body_key(
            content_version_key=version_key,
            body_role="primary",
            section_path=None,
            normalized_body_hash="BBBB",
        )
        asset_key = make_content_asset_key(
            content_version_key=version_key,
            file_name="Chart 1.PNG",
            asset_hash=None,
        )
        entity_key = make_content_entity_link_key(
            content_version_key=version_key,
            entity_type="investment",
            entity_name="Example Investment A",
            content_body_id="CONTENT_BODY_abc123",
            mention_start=12,
            mention_end=32,
        )
        date_key = make_content_date_key(
            content_version_key=version_key,
            date_type="report period end",
            date_value="2025-12-31",
        )
        scope_key = make_content_scope_key(
            content_version_key=version_key,
            scope_type="strategy",
            scope_entity_name="PEP Weekly",
        )

        self.assertEqual(body_key, f"{version_key}:body:PRIMARY:document:bbbb")
        self.assertEqual(asset_key, f"{version_key}:asset:chart 1.png:no_hash")
        self.assertEqual(
            entity_key,
            f"{version_key}:entity:INVESTMENT:example investment a:CONTENT_BODY_abc123:12-32",
        )
        self.assertEqual(date_key, f"{version_key}:date:REPORT_PERIOD_END:2025-12-31")
        self.assertEqual(scope_key, f"{version_key}:scope:STRATEGY:pep weekly")

    def test_content_fingerprint_is_stable_for_same_normalized_body_and_assets(self) -> None:
        first_body_hash = make_body_hash("# Note\r\n\r\nLine one.   \r\n")
        second_body_hash = make_body_hash("\n# Note\n\nLine one.\n\n")
        asset_fingerprint = make_asset_fingerprint(["BBBB", "aaaa"])

        self.assertEqual(first_body_hash, second_body_hash)
        self.assertEqual(
            make_content_fingerprint(body_hash=first_body_hash, asset_fingerprint=asset_fingerprint),
            make_content_fingerprint(body_hash=second_body_hash, asset_fingerprint=asset_fingerprint),
        )

    def test_deterministic_ids_are_stable_and_namespaced(self) -> None:
        business_key = "SHAREPOINT_ONLINE:sp-note-001"

        self.assertEqual(
            make_deterministic_id("content_item", business_key),
            make_deterministic_id("CONTENT ITEM", business_key),
        )
        self.assertTrue(make_deterministic_id("content_item", business_key).startswith("CONTENT_ITEM_"))


if __name__ == "__main__":
    unittest.main()
