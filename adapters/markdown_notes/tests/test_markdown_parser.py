from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.bundle_reader import read_bundle
from aps_markdown_notes.markdown_parser import parse_bundle


class MarkdownParserTests(unittest.TestCase):
    def test_every_successfully_parsed_note_emits_primary_body(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            (temp_path / "bundles/note.md").parent.mkdir(parents=True, exist_ok=True)
            (temp_path / "bundles/note.md").write_text("# Weekly Note\n\nHello world.\n", encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": "weekly-note",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-weekly-note",
                        "source_type": "NOTE",
                        "source_title": "Weekly Note",
                    },
                    "content": {"markdown_path": "bundles/note.md"},
                },
                temp_path,
            )
            parsed = parse_bundle(bundle)

            self.assertEqual(parsed.primary_body.body_role, "PRIMARY")
            self.assertTrue(parsed.primary_body.text)
            self.assertEqual(len(parsed.bodies), 2)


if __name__ == "__main__":
    unittest.main()

