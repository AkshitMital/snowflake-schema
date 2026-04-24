# APS Markdown Note Pilot Results

## Recommendation

- Status: `<ready for app build | needs adapter fixes | needs schema/contract fixes>`
- Notes evaluated: `<count>`
- Labeled bundles evaluated: `<count>`
- Report generated at: `<timestamp>`

### Rationale

- `<decision rationale 1>`
- `<decision rationale 2>`

## Issue Summary

- Duplicate item key issues: `<count>`
- Duplicate version key issues: `<count>`
- Missing primary body issues: `<count>`
- Unresolved entity mention issues: `<count>`
- Missing investment body anchor issues: `<count>`
- Missing portfolio body anchor issues: `<count>`
- Missing strategy body anchor issues: `<count>`
- Missing operational date issues: `<count>`

## Derived Note Counts

- Investment notes: `<count>`
- Portfolio notes: `<count>`
- Strategy notes: `<count>`

## Duplicate Item/Version Key Issues

- `<example or None>`

## Missing Primary Body Issues

- `<example or None>`

## Unresolved Entity Mentions

- `<example or None>`

## Missing Body Anchors

- `<example or None>`

## Missing Operational Dates

- `<example or None>`

## False Positive Entity-Link Examples

- `<example or None>`

## False Negative Entity-Link Examples

- `<example or None>`

## Notes Blocked From App Promotion By P1 Issues

- `<example or None>`

## Notes

- Use `adapters/markdown_notes/sql/005_validation_metrics.sql` to extract Snowflake metrics for the pilot ingestion run.
- Use `aps_markdown_notes.validation_report` to merge those metrics with labeled manifest expectations and render the final markdown report.
- If no labeled subset is available, default the recommendation to `needs adapter fixes` until false-positive and false-negative examples can be measured.

