from __future__ import annotations

import json
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
from aps_markdown_notes.staging_writer import build_staging_batch, simulate_merge_batch
from aps_markdown_notes.validation_report import (
    STRUCTURAL_SCHEMA_CONTRACT_CODES,
    ReviewIssue,
    build_local_validation_summary,
    render_validation_report,
)


class ValidationReportTests(unittest.TestCase):
    def test_rendered_report_includes_required_sections(self) -> None:
        summary = self._build_summary()
        markdown = render_validation_report(summary)

        self.assertIn("## Duplicate Item/Version Key Issues", markdown)
        self.assertIn("## Missing Primary Body Issues", markdown)
        self.assertIn("## Unresolved Entity Mentions", markdown)
        self.assertIn("## Missing Body Anchors", markdown)
        self.assertIn("## Missing Operational Dates", markdown)
        self.assertIn("## Derived Note Counts", markdown)
        self.assertIn("## False Positive Entity-Link Examples", markdown)
        self.assertIn("## False Negative Entity-Link Examples", markdown)
        self.assertIn("## Notes Blocked From App Promotion By P1 Issues", markdown)

    def test_summary_detects_false_positive_and_false_negative_examples(self) -> None:
        summary = self._build_summary()

        self.assertEqual(summary.labeled_bundle_count, 2)
        self.assertGreaterEqual(len(summary.false_positive_examples), 1)
        self.assertGreaterEqual(len(summary.false_negative_examples), 1)
        self.assertEqual(summary.recommendation, "needs adapter fixes")

    def test_structural_issue_forces_schema_contract_recommendation(self) -> None:
        summary = self._build_summary(
            extra_review_issues=[
                ReviewIssue(
                    content_id="content-1",
                    content_version_id=None,
                    issue_code=next(iter(STRUCTURAL_SCHEMA_CONTRACT_CODES)),
                    severity="P1",
                    issue_detail="Structural duplicate/current-pointer issue.",
                    source_title="Structural Test Note",
                )
            ]
        )

        self.assertEqual(summary.recommendation, "needs schema/contract fixes")

    def _build_summary(self, extra_review_issues: list[ReviewIssue] | None = None):
        manifest = {
            "bundles": [
                {
                    "bundle_id": "bundle-strategy",
                    "expected": {
                        "entities": [
                            {"entity_type": "TOPIC", "entity_name": "Rates"}
                        ]
                    }
                },
                {
                    "bundle_id": "bundle-multi",
                    "expected": {
                        "entities": [
                            {"entity_type": "INVESTMENT", "entity_name": "Acme Solar"},
                            {"entity_type": "INVESTMENT", "entity_name": "Beta Grid"}
                        ]
                    }
                }
            ]
        }

        state = None
        scenarios = [
            (
                "bundle-strategy",
                "# PEP Weekly\n\nPEP Weekly focused on rates.\n",
                {
                    "source_system": "SharePoint Online",
                    "source_object_id": "sp-pep-weekly",
                    "source_type": "NOTE",
                    "source_title": "PEP Weekly",
                },
                {"markdown_path": "bundles/note.md", "source_file_hash": "1" * 64},
                {"STRATEGY": {"PEP Weekly": "strategy_pep_weekly"}},
            ),
            (
                "bundle-multi",
                "# Multi Investment Commentary\n\n## Investments\n### Acme Solar\nAcme Solar had a strong quarter.\n\n### UnknownCo Infrastructure\nUnknownCo management updated us.\n",
                {
                    "source_system": "SharePoint Online",
                    "source_object_id": "sp-multi-investment",
                    "source_type": "NOTE",
                    "source_title": "Multi Investment Commentary",
                },
                {"markdown_path": "bundles/note.md", "source_file_hash": "2" * 64},
                {
                    "INVESTMENT": {
                        "Acme Solar": "inv_acme_solar",
                        "Gamma Wind": "inv_gamma_wind"
                    }
                },
            ),
        ]

        for index, (bundle_id, markdown, source, content, entity_catalog) in enumerate(scenarios, start=1):
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                note_path = temp_path / content["markdown_path"]
                note_path.parent.mkdir(parents=True, exist_ok=True)
                note_path.write_text(markdown, encoding="utf-8")

                bundle = read_bundle(
                    {
                        "bundle_id": bundle_id,
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
                batch = build_staging_batch(
                    bundle=bundle,
                    parsed_document=parsed,
                    assets=assets,
                    scopes=scopes,
                    entity_links=entity_links,
                    dates=dates,
                    ingestion_run_id=f"run-{index}",
                    adapter_version="0.1.0-test",
                )
                state = simulate_merge_batch(state, batch)

        return build_local_validation_summary(
            state,
            manifest=manifest,
            review_issues=extra_review_issues,
            report_title="Synthetic APS Pilot Validation",
        )


if __name__ == "__main__":
    unittest.main()
