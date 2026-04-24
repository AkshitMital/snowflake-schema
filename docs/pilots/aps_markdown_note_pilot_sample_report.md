# Synthetic APS Pilot Validation (Pre-Pilot Example)

## Recommendation

- Status: `needs adapter fixes`
- Notes evaluated: `2`
- Labeled bundles evaluated: `2`
- Report generated at: `2026-04-24T10:12:27Z`

### Rationale
- The labeled subset still shows entity-link false positives and/or false negatives.
- Resolve entity-link quality issues before using the review app as a primary operational surface.

## Issue Summary

- Duplicate item key issues: `0`
- Duplicate version key issues: `0`
- Missing primary body issues: `0`
- Unresolved entity mention issues: `1`
- Missing investment body anchor issues: `0`
- Missing portfolio body anchor issues: `0`
- Missing strategy body anchor issues: `0`
- Missing operational date issues: `0`

## Derived Note Counts

- Investment notes: `1`
- Portfolio notes: `0`
- Strategy notes: `2`

## Duplicate Item/Version Key Issues

- None

## Missing Primary Body Issues

- None

## Unresolved Entity Mentions

- `UNRESOLVED_ENTITY_MENTIONS` on `Multi Investment Commentary`: Entity mentions have names but no resolved entity ids.

## Missing Body Anchors

- None

## Missing Operational Dates

- None

## False Positive Entity-Link Examples

- `PEP Weekly` [STRATEGY]: expected `n/a`, actual `pep weekly`. Resolved entity link is not present in the labeled expected set for this bundle.

## False Negative Entity-Link Examples

- `PEP Weekly` [TOPIC]: expected `rates`, actual `n/a`. Expected entity is missing from resolved entity links.
- `Multi Investment Commentary` [INVESTMENT]: expected `beta grid`, actual `n/a`. Expected entity is missing from resolved entity links.

## Notes Blocked From App Promotion By P1 Issues

- None
