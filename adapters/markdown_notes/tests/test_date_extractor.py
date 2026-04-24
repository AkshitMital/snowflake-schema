from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.bundle_reader import read_bundle
from aps_markdown_notes.date_extractor import extract_date_candidates
from aps_markdown_notes.markdown_parser import parse_bundle


class DateExtractorTests(unittest.TestCase):
    def test_multiple_meaningful_date_candidates_are_preserved(self) -> None:
        markdown = """# Strategy Note

Meeting Date: January 15, 2026
Period: 2025-10-01 to 2025-12-31
As of 2026-01-31
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            note_path = temp_path / "bundles/dates.md"
            note_path.parent.mkdir(parents=True, exist_ok=True)
            note_path.write_text(markdown, encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": "date-heavy-note",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-date-heavy-note",
                        "source_type": "NOTE",
                        "source_title": "Strategy Note",
                        "source_created_at": "2026-01-10T09:00:00Z",
                        "source_modified_at": "2026-01-16T18:30:00Z"
                    },
                    "content": {"markdown_path": "bundles/dates.md"},
                },
                temp_path,
                ingested_at="2026-04-24T12:00:00Z",
            )
            parsed = parse_bundle(bundle)
            dates = extract_date_candidates(bundle, parsed)
            date_types = {candidate.date_type for candidate in dates}

            self.assertIn("MEETING_DATE", date_types)
            self.assertIn("REPORT_PERIOD_START", date_types)
            self.assertIn("REPORT_PERIOD_END", date_types)
            self.assertIn("EFFECTIVE_DATE_CANDIDATE", date_types)
            self.assertIn("SOURCE_CREATED", date_types)
            self.assertIn("SOURCE_MODIFIED", date_types)
            self.assertIn("INGESTED", date_types)


if __name__ == "__main__":
    unittest.main()

