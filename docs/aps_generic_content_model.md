# APS Generic Content Canonical Model

## Decision

`INVESTMENT_NOTES` is not the canonical intake model for broad PM notes,
commentary, portfolio updates, strategy notes, markdown exports, or image-heavy
content.

The canonical intake model is `APS_CORE.CONTENT_*`.

Investment notes are derived from generic content only when entity resolution is
strong enough. That keeps the investment model clean and prevents source documents
from being flattened into fake one-investment notes.

## Why This Exists

The existing QPR and valuation-report pipelines work because those sources are
structured and investment-centric. They naturally produce records like:

- `INVESTMENT_NAME`
- `PORTFOLIO_NAME`
- `EFFECTIVE_DATE`
- `SOURCE_TYPE`
- `SOURCE_PROVENANCE`
- `SOURCE_URL`
- `SUMMARY`

The broader notes corpus does not have that shape. Some notes have no investment.
Some mention five investments. Some are really strategy notes. Some are portfolio
updates. Some are image-heavy webcasts where the chart matters as much as the text.

Forcing those documents directly into `INVESTMENT_NOTES` would create information
loss and false precision.

## Canonical Tables

`CONTENT_ITEMS` stores one logical source object. A logical object can be a PM note,
strategy note, portfolio update, markdown export, webcast, or future source.

`CONTENT_VERSIONS` stores exports, re-exports, ingestion runs, fingerprints,
duplicate detection, and version lineage.

`CONTENT_BODIES` stores normalized text, markdown, HTML, OCR text, transcripts,
sections, and body hashes.

`CONTENT_ASSETS` stores images, embedded files, charts, screenshots, OCR/caption
text, chart data, asset hashes, and anchors back to body sections.

`CONTENT_SCOPES` stores the intended scope of the content: strategy, portfolio,
investment, multi-investment, market, firm, unknown, or mixed.

`CONTENT_ENTITY_LINKS` stores many-to-many links from content to investments,
portfolios, strategies, companies, people, funds, topics, and future entity types.
Every link has confidence, evidence, and review status.

`CONTENT_DATES` stores source dates, inferred dates, meeting dates, report-period
dates, and effective-date candidates. It does not force a single effective date.

## Derived Views

`APS_CORE.INVESTMENT_NOTES_FROM_CONTENT` emits investment-note-shaped rows only
when:

- The content has an `INVESTMENT` entity link.
- The link confidence is at least `0.85`.
- The link review status is `AUTO_ACCEPTED` or `APPROVED`.
- The content item and version are active/current.

The view includes `SOURCE_CONTENT_ID`, `SOURCE_CONTENT_VERSION_ID`, and
`SOURCE_ENTITY_LINK_ID` so downstream users can trace every derived row back to the
canonical source object and the exact entity-resolution decision.

It also includes `DERIVATION_RULE_VERSION` so changes to derivation thresholds or
date/body selection rules can be audited over time.

`APS_MART.INVESTMENT_NOTE_TIMELINE_FROM_CONTENT` exposes the derived investment-note
timeline for app and reporting consumers.

`APS_MART.CONTENT_REVIEW_QUEUE` surfaces ambiguous content that needs review:
low-confidence investment links, unresolved entity mentions, missing date
candidates, and image-heavy notes without extracted asset context.

## Canonical vs Derived

Canonical data preserves source truth:

- Source identity and provenance.
- Raw-to-normalized lineage.
- Bodies, sections, assets, captions, OCR, and chart data.
- Dedupe/version fingerprints.
- Entity mentions and resolved links with confidence.
- Date candidates with evidence.
- Scope classification with confidence.

Derived data is an interpretation:

- Per-investment notes.
- Per-investment summaries.
- Portfolio timelines.
- Strategy timelines.
- KPI/assertion extraction.
- Sentiment, themes, and app-facing rollups.
- A chosen effective date.

Summaries are not canonical source truth. If summaries are persisted, they should be
tagged as derived content with method/model metadata and confidence.

## Edge Cases

No investment name: store the content item anyway. It is valid content, not a
failed investment note.

Multiple investments: store one content item with many entity links. Derive one
investment-note row per high-confidence investment link.

Strategy-level notes: classify the content scope as `STRATEGY` and link to a
strategy entity or controlled label.

Portfolio-level notes: link to portfolio or fund entities without inventing an
investment.

Undated notes: keep the effective date null. Store source created/modified/ingested
timestamps and any date candidates separately.

Image-heavy notes: store assets as first-class rows with hashes, source/storage
URIs, captions, OCR text, chart data, and body anchors.

Duplicate or re-exported notes: connect them through `CONTENT_VERSIONS` using body,
asset, and content fingerprints. Do not dedupe by URL alone.

Future sources: adapters should normalize into the same content model whether the
input is markdown, PDF, email, SharePoint, webcast, manual upload, or something
else.

## MVP Ingestion Contract

A markdown-note adapter should produce:

- One `CONTENT_ITEMS` row per logical source object.
- One `CONTENT_VERSIONS` row per export or re-export.
- At least one `CONTENT_BODIES` row with normalized text or markdown.
- Zero or more `CONTENT_ASSETS` rows for linked images or embedded files.
- Zero or more `CONTENT_SCOPES` rows.
- Zero or more `CONTENT_ENTITY_LINKS` rows.
- Zero or more `CONTENT_DATES` rows.

The adapter should not drop a note just because it has no investment or no date.
Those are normal states in this model.

## MVP Recommendation

Start with markdown-converted notes, but do not make markdown the model.

Implement ingestion into `APS_CORE.CONTENT_*`, preserve assets, calculate body and
asset fingerprints, record entity links with confidence, record date candidates,
and route ambiguous records to review. Then let investment-facing consumers read
from `APS_CORE.INVESTMENT_NOTES_FROM_CONTENT` or the mart timeline view.

That is the wedge: generic enough to survive future sources, concrete enough to
ship now.
