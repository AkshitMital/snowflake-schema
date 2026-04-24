from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.bundle_reader import read_bundle
from aps_markdown_notes.markdown_parser import parse_bundle


class BodySectionTests(unittest.TestCase):
    def test_section_body_extraction_preserves_nested_section_paths(self) -> None:
        markdown = """# Portfolio Update

Intro text.

## Investments
### Acme Solar
Strong quarter.

### Beta Grid
Stable quarter.
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundle_path = temp_path / "bundles/portfolio.md"
            bundle_path.parent.mkdir(parents=True, exist_ok=True)
            bundle_path.write_text(markdown, encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": "portfolio-update",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-portfolio-update",
                        "source_type": "NOTE",
                        "source_title": "Portfolio Update",
                    },
                    "content": {"markdown_path": "bundles/portfolio.md"},
                },
                temp_path,
            )
            parsed = parse_bundle(bundle)
            section_paths = [body.section_path for body in parsed.section_bodies]

            self.assertIn("portfolio-update", section_paths)
            self.assertIn("portfolio-update/investments", section_paths)
            self.assertIn("portfolio-update/investments/acme-solar", section_paths)
            self.assertIn("portfolio-update/investments/beta-grid", section_paths)


if __name__ == "__main__":
    unittest.main()

