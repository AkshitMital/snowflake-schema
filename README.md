# BLK Snowflake Schema

This repository contains APS Snowflake schema artifacts.

## APS Generic Content Model v2

The schema package adds a source-agnostic canonical content layer
for broad APS note-like sources: PM meeting notes, investment commentary, portfolio
updates, strategy notes, markdown exports, and image-heavy documents.

The model preserves content as content first. Investment-specific notes are derived
only when entity resolution is strong enough.

## File Layout

- `sql/aps_core/001_content_canonical_model.sql` creates the canonical
  `APS_CORE.CONTENT_*` tables.
- `sql/aps_core/002_investment_notes_from_content.sql` creates the conservative
  current-version, date-selection, investment, portfolio, and strategy derivation
  views.
- `sql/aps_core/003_content_quality_views.sql` creates canonical review issue
  diagnostics.
- `sql/aps_mart/001_content_marts.sql` creates mart-facing timeline and review queue
  views.
- `examples/acceptance_queries.sql` contains scenario-oriented validation queries.
- `docs/aps_generic_content_model.md` explains the architecture and tradeoffs.
- `docs/aps_content_ingestion_contract.md` defines adapter keys, merge order,
  current-version promotion, and quarantine rules.

## Apply Order

Run the SQL files in this order:

```sql
-- Canonical content tables.
@sql/aps_core/001_content_canonical_model.sql;

-- Conservative investment-note derivation.
@sql/aps_core/002_investment_notes_from_content.sql;

-- Canonical quality/review diagnostics.
@sql/aps_core/003_content_quality_views.sql;

-- Mart-facing views.
@sql/aps_mart/001_content_marts.sql;
```

## Design Rule

Do not force broad notes directly into `INVESTMENT_NOTES`.

Store the source object, versions, body text, assets, scopes, entity links, and date
candidates canonically in `APS_CORE.CONTENT_*`. Derive investment notes only when
there is a high-confidence investment entity link.

For real ingestion, adapters must provide deterministic business keys and maintain
`APS_CORE.CONTENT_ITEM_CURRENT_VERSION`. Snowflake standard-table constraints are
not enough to prevent duplicate current versions.
