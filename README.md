# BLK Snowflake Schema

This repository contains APS Snowflake schema artifacts.

## APS Generic Content Model

The first implemented schema package adds a source-agnostic canonical content layer
for broad APS note-like sources: PM meeting notes, investment commentary, portfolio
updates, strategy notes, markdown exports, and image-heavy documents.

The model preserves content as content first. Investment-specific notes are derived
only when entity resolution is strong enough.

## File Layout

- `sql/aps_core/001_content_canonical_model.sql` creates the canonical
  `APS_CORE.CONTENT_*` tables.
- `sql/aps_core/002_investment_notes_from_content.sql` creates the conservative
  derived investment-note view.
- `sql/aps_mart/001_content_marts.sql` creates mart-facing timeline and review
  queue views.
- `examples/acceptance_queries.sql` contains scenario-oriented validation queries.
- `docs/aps_generic_content_model.md` explains the architecture and tradeoffs.

## Apply Order

Run the SQL files in this order:

```sql
-- Canonical content tables.
@sql/aps_core/001_content_canonical_model.sql;

-- Conservative investment-note derivation.
@sql/aps_core/002_investment_notes_from_content.sql;

-- Mart-facing views.
@sql/aps_mart/001_content_marts.sql;
```

## Design Rule

Do not force broad notes directly into `INVESTMENT_NOTES`.

Store the source object, versions, body text, assets, scopes, entity links, and date
candidates canonically in `APS_CORE.CONTENT_*`. Derive investment notes only when
there is a high-confidence investment entity link.
