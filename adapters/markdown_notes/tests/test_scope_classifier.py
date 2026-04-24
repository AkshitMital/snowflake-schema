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
from aps_markdown_notes.scope_classifier import classify_scopes


class ScopeClassifierTests(unittest.TestCase):
    def test_strategy_portfolio_and_investment_scopes_are_distinguished(self) -> None:
        scenarios = [
            (
                "PEP Weekly",
                "# PEP Weekly\n\nPEP Weekly focused on rates.\n",
                {"STRATEGY": {"PEP Weekly": "strategy_pep_weekly"}},
                "STRATEGY",
            ),
            (
                "SAIF Portfolio Redemption",
                "# SAIF Portfolio Redemption\n\nSAIF saw liquidity improve.\n",
                {"PORTFOLIO": {"SAIF": "portfolio_saif"}},
                "PORTFOLIO",
            ),
            (
                "Acme Solar Update",
                "# Acme Solar Update\n\nAcme Solar continues to perform.\n",
                {"INVESTMENT": {"Acme Solar": "inv_acme_solar"}},
                "INVESTMENT",
            ),
        ]

        for title, markdown, catalog, expected_scope in scenarios:
            with self.subTest(title=title):
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = Path(temp_dir)
                    note_path = temp_path / "bundles/note.md"
                    note_path.parent.mkdir(parents=True, exist_ok=True)
                    note_path.write_text(markdown, encoding="utf-8")

                    bundle = read_bundle(
                        {
                            "bundle_id": title.lower().replace(" ", "-"),
                            "source": {
                                "source_system": "SharePoint Online",
                                "source_object_id": f"sp-{title.lower().replace(' ', '-')}",
                                "source_type": "NOTE",
                                "source_title": title,
                            },
                            "content": {"markdown_path": "bundles/note.md"},
                        },
                        temp_path,
                    )
                    parsed = parse_bundle(bundle)
                    links = link_entities(parsed, entity_catalog=catalog)
                    scopes = classify_scopes(parsed, links)

                    self.assertEqual(scopes[0].scope_type, expected_scope)


if __name__ == "__main__":
    unittest.main()

