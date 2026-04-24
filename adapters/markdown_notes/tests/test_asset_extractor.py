from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aps_markdown_notes.asset_extractor import extract_assets
from aps_markdown_notes.bundle_reader import read_bundle
from aps_markdown_notes.markdown_parser import parse_bundle


class AssetExtractorTests(unittest.TestCase):
    def test_asset_extraction_and_anchoring_follow_section_reference(self) -> None:
        markdown = """# Decarb Partners Fund I Q4 2025 Webcast

## Portfolio Overview
![Slide 1](assets/slide-001.png)
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            note_path = temp_path / "bundles/webcast/index.md"
            note_path.parent.mkdir(parents=True, exist_ok=True)
            note_path.write_text(markdown, encoding="utf-8")

            asset_path = temp_path / "bundles/webcast/assets/slide-001.png"
            asset_path.parent.mkdir(parents=True, exist_ok=True)
            asset_path.write_bytes(b"fake-image")
            (temp_path / "bundles/webcast/assets/slide-001.ocr.txt").write_text("Slide 1 OCR", encoding="utf-8")

            bundle = read_bundle(
                {
                    "bundle_id": "decarb-webcast",
                    "source": {
                        "source_system": "SharePoint Online",
                        "source_object_id": "sp-decarb-webcast",
                        "source_type": "WEBCAST",
                        "source_title": "Decarb Partners Fund I Q4 2025 Webcast",
                    },
                    "content": {
                        "markdown_path": "bundles/webcast/index.md",
                        "assets": [
                            {
                                "asset_path": "bundles/webcast/assets/slide-001.png",
                                "file_name": "slide-001.png",
                                "asset_type": "SCREENSHOT",
                                "ocr_text_path": "bundles/webcast/assets/slide-001.ocr.txt",
                            }
                        ],
                    },
                },
                temp_path,
            )
            parsed = parse_bundle(bundle)
            assets = extract_assets(bundle, parsed)

            self.assertEqual(len(assets), 1)
            self.assertEqual(assets[0].asset_type, "SCREENSHOT")
            self.assertEqual(assets[0].extraction_status, "AVAILABLE")
            self.assertEqual(assets[0].ocr_text, "Slide 1 OCR")
            self.assertEqual(assets[0].anchor_section_path, "decarb-partners-fund-i-q4-2025-webcast/portfolio-overview")


if __name__ == "__main__":
    unittest.main()

