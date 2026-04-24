from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.bundle_reader import read_bundle
from aps_markdown_notes.entity_linker import link_entities
from aps_markdown_notes.markdown_parser import parse_bundle


class EntityLinkerTests(unittest.TestCase):
    def test_multi_investment_links_are_body_anchored(self) -> None:
        markdown = """# Multi Investment Commentary

## Investments
### Acme Solar
Acme Solar had a strong quarter.

### Beta Grid
Beta Grid stabilized operating margins.
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            note_path = temp_path / "bundles/multi.md"
            note_path.parent.mkdir(parents=True, exist_ok=True)
            note_path.write_text(markdown, encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": "multi-investment",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-multi-investment",
                        "source_type": "NOTE",
                        "source_title": "Multi Investment Commentary",
                    },
                    "content": {"markdown_path": "bundles/multi.md"},
                },
                temp_path,
            )
            parsed = parse_bundle(bundle)
            links = link_entities(
                parsed,
                entity_catalog={
                    "INVESTMENT": {
                        "Acme Solar": "inv_acme_solar",
                        "Beta Grid": "inv_beta_grid",
                    }
                },
            )

            primary_body_id = parsed.primary_body.body_id
            investment_links = [link for link in links if link.entity_type == "INVESTMENT"]

            self.assertEqual(len(investment_links), 4)
            self.assertTrue(all(link.content_body_id != primary_body_id for link in investment_links if link.link_role == "PRIMARY_SUBJECT"))

    def test_unresolved_mentions_are_preserved_without_faking_ids(self) -> None:
        markdown = """# Portfolio Update

## Investments
### UnknownCo Infrastructure
We met the management team this week.
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            note_path = temp_path / "bundles/unresolved.md"
            note_path.parent.mkdir(parents=True, exist_ok=True)
            note_path.write_text(markdown, encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": "unresolved-mention-note",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-unresolved-note",
                        "source_type": "NOTE",
                        "source_title": "Portfolio Update",
                    },
                    "content": {"markdown_path": "bundles/unresolved.md"},
                },
                temp_path,
            )
            parsed = parse_bundle(bundle)
            links = link_entities(parsed, entity_catalog={"INVESTMENT": {"Acme Solar": "inv_acme_solar"}})

            unresolved = [link for link in links if link.entity_id is None]

            self.assertEqual(len(unresolved), 1)
            self.assertEqual(unresolved[0].entity_name, "UnknownCo Infrastructure")
            self.assertEqual(unresolved[0].entity_type, "INVESTMENT")
            self.assertEqual(unresolved[0].review_status, "NEEDS_REVIEW")


if __name__ == "__main__":
    unittest.main()

