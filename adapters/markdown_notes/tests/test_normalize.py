from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.normalize import (
    hash_markdown_text,
    normalize_markdown_text,
    normalize_source_path,
    normalize_source_system,
    normalize_source_uri,
)


class NormalizeTests(unittest.TestCase):
    def test_source_system_normalization_is_case_and_whitespace_stable(self) -> None:
        self.assertEqual(normalize_source_system(" SharePoint   Online "), "SHAREPOINT_ONLINE")
        self.assertEqual(normalize_source_system("sharepoint-online"), "SHAREPOINT_ONLINE")

    def test_source_path_normalization_decodes_and_cleans_paths(self) -> None:
        self.assertEqual(
            normalize_source_path("\\Strategy//PEP%20Weekly.md "),
            "strategy/pep weekly.md",
        )

    def test_source_uri_normalization_drops_transient_query_and_fragment(self) -> None:
        self.assertEqual(
            normalize_source_uri(
                "HTTPS://Tenant.SharePoint.com/sites/APS/Shared%20Documents//Strategy/PEP%20Weekly.md?download=1#page"
            ),
            "https://tenant.sharepoint.com/sites/aps/shared documents/strategy/pep weekly.md",
        )

    def test_markdown_normalization_ignores_trailing_whitespace_and_line_endings(self) -> None:
        left = "# Note\r\n\r\nLine one.   \r\nLine two.\r\n"
        right = "\n# Note\n\nLine one.\nLine two.   \n\n"

        self.assertEqual(normalize_markdown_text(left), normalize_markdown_text(right))
        self.assertEqual(hash_markdown_text(left), hash_markdown_text(right))

    def test_markdown_hash_changes_for_material_text_change(self) -> None:
        self.assertNotEqual(
            hash_markdown_text("# Note\n\nLine one."),
            hash_markdown_text("# Note\n\nLine two."),
        )


if __name__ == "__main__":
    unittest.main()

