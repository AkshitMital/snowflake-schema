# APS Generic Content Canonical Model v2

## Decision

`INVESTMENT_NOTES` is not the canonical intake model for broad PM notes,
commentary, portfolio updates, strategy notes, markdown exports, or image-heavy
content.

The canonical intake model is `APS_CORE.CONTENT_*`.

Investment, portfolio, and strategy timelines are derived from generic content only
when current-version, entity-resolution, body-anchor, and date-selection rules are
deterministic enough for production use.

## Critique of v1

The v1 model got the main product decision right: content first, investment notes
derived later. It was not production-safe yet.

1. `CONTENT_ITEMS.LOGICAL_SOURCE_KEY` was not enough. It did not define a stable
   business key or explain moved files, re-exports, or source object ids.
2. `CONTENT_VERSIONS.IS_CURRENT` defaulted to true. That makes duplicate current
   versions easy. Snowflake standard-table primary and unique keys do not protect
   you from this.
3. Derivation used the best whole-document body as a fallback too often. For
   multi-investment or mixed-scope notes, that risks giving every investment the
   same generic note text.
4. There was no compatibility projection for the existing `INVESTMENT_NOTES` shape.
   `SOURCE_SHAREPOINT` was missing, and traceability fields were mixed into the
   same view without a clean app-facing contract.
5. Portfolio and strategy derivations were deferred even though the canonical model
   explicitly supports portfolio-level and strategy-level content.
6. The review queue only caught a few soft issues. It missed duplicate current
   versions, duplicate-version conflicts, scope conflicts, missing primary bodies,
   missing reviewed links, missing section anchors, and image-heavy notes without
   extraction context.
7. Date candidates existed, but no single selected operational date was defined.
   App timelines need one deterministic date field while preserving all candidates.

## Item vs Version Rules

`CONTENT_ITEM` means one logical source object.

`CONTENT_VERSION` means one observed normalized state of that logical object.

Create a new `CONTENT_ITEM` when:

- The source system gives a new stable object id.
- There is no stable object id and the normalized source container/path points to a
  different logical object.
- Duplicate detection finds the same content under a different key, but the adapter
  cannot safely prove it is the same source object. In that case create the item and
  set `DUPLICATE_OF_CONTENT_ID`.

Create a new `CONTENT_VERSION` when:

- The same item has a new `CONTENT_FINGERPRINT`.
- The source has a new immutable version id.
- The body, assets, OCR/captions, or normalized extraction output changed.
- The adapter is reprocessing with materially different extraction behavior.

Do not create a new current version when:

- A file was re-exported with the same `CONTENT_FINGERPRINT`.
- A file moved but has the same source object id and same fingerprint.
- Only source URI/path casing changed.

Examples:

- Same SharePoint file id, same fingerprint, new export timestamp: same item, no new
  current version. Optionally record a duplicate/re-export version with
  `VERSION_STATUS = 'DUPLICATE'`.
- Same SharePoint file id, changed markdown text: same item, new current version,
  previous version becomes superseded.
- Same file moved from `/weekly/` to `/archive/`, same source object id: same item,
  update item source path/URI, no new current version if fingerprint is unchanged.
- Same content appears in a new manually uploaded file with no source object id:
  new item marked `DUPLICATE_OF_CONTENT_ID` after reconciliation.
- PEP Weekly for a new week: new item, because it is a new logical note.

## Business Keys and Idempotency

The adapter must produce deterministic merge keys:

- `CONTENT_ITEM_KEY`: source-system namespaced logical item key.
- `CONTENT_VERSION_KEY`: item key plus source version id or content fingerprint.
- `CONTENT_BODY_KEY`: version key plus body role, section path, and body hash.
- `CONTENT_ASSET_KEY`: version key plus asset path/URI and asset hash.
- `CONTENT_SCOPE_KEY`: version key plus scope type and scoped entity.
- `CONTENT_ENTITY_LINK_KEY`: version key plus entity type, entity id/name, anchor,
  and mention span.
- `CONTENT_DATE_KEY`: version key plus date type, value/period, and body anchor.

Use deterministic ids derived from those keys when possible. For example,
`CONTENT_ID = SHA2('CONTENT_ITEM|' || CONTENT_ITEM_KEY, 256)`.

The current version is maintained in two places:

- `APS_CORE.CONTENT_ITEM_CURRENT_VERSION`: the production pointer.
- `APS_CORE.CONTENT_VERSIONS.IS_CURRENT`: denormalized for debugging and ad hoc
  analysis.

Production views use `CONTENT_ITEM_CURRENT_VERSION`. The review queue flags any
divergence, including multiple current flags or missing pointers.

## Derivation Compatibility

The clean compatibility layer is:

- `APS_CORE.INVESTMENT_NOTES_FROM_CONTENT`: rich derived view with traceability,
  confidence, source body, date source, and derivation status.
- `APS_CORE.INVESTMENT_NOTES_COMPAT_FROM_CONTENT`: projection matching the current
  investment-note shape plus minimal traceability columns.
- `APS_MART.INVESTMENT_NOTE_TIMELINE_FROM_CONTENT`: app/reporting timeline view.

Current shape mapping:

| Current field | v2 source |
| --- | --- |
| `INVESTMENT_NAME` | approved `CONTENT_ENTITY_LINKS` investment entity name |
| `PORTFOLIO_NAME` | approved portfolio/fund context links |
| `EFFECTIVE_DATE` | selected operational date |
| `SOURCE_TYPE` | `CONTENT_CURRENT_VERSIONS.SOURCE_TYPE` |
| `SOURCE_PROVENANCE` | object containing content id, version id, link id, source ids, adapter, raw record |
| `SOURCE_URL` | `CONTENT_CURRENT_VERSIONS.SOURCE_URI` |
| `SOURCE_SHAREPOINT` | source URI when `SOURCE_SYSTEM = 'SHAREPOINT'` |
| `SOURCE_REFERENCE` | `CONTENT_CURRENT_VERSIONS.SOURCE_REFERENCE` |
| `SUMMARY` | body-anchored text when required, otherwise primary document body |

Important type note: `SOURCE_PROVENANCE` is a `VARIANT` object in v2. If the existing
`INVESTMENT_NOTES` table expects a string, keep the compatibility view and cast or
serialize in the final insertion layer. Do not flatten provenance too early.

## Multi-Investment and Section Anchoring

Entity links must anchor to the most specific evidence available:

- Document-level primary subject: `CONTENT_BODY_ID` can be null only if the note has
  one approved investment subject and the link role is `PRIMARY_SUBJECT` or
  `DOCUMENT_SUBJECT`.
- Section-level subject: set `CONTENT_BODY_ID` to the section body and use
  `LINK_ROLE = 'SECTION_SUBJECT'`.
- Mention-only link: set `CONTENT_BODY_ID`, `MENTION_START`, and `MENTION_END` when
  available. Mention-only links do not become investment notes unless promoted or
  reviewed.
- Multi-investment or mixed-scope note: every derived investment note must have a
  body anchor. No whole-note fallback.

This is the most important hardening rule. Without it, a five-investment meeting note
becomes five copies of the same generic summary. That looks useful in a demo and
then poisons the timeline.

## Portfolio and Strategy Derivations

Portfolio and strategy content are first-class derived surfaces:

- `APS_CORE.PORTFOLIO_NOTES_FROM_CONTENT`
- `APS_CORE.STRATEGY_NOTES_FROM_CONTENT`
- `APS_MART.PORTFOLIO_NOTE_TIMELINE_FROM_CONTENT`
- `APS_MART.STRATEGY_NOTE_TIMELINE_FROM_CONTENT`

Portfolio derivation uses approved `PORTFOLIO` or `FUND` links at confidence `0.80`
or higher.

Strategy derivation uses approved `STRATEGY` or `TOPIC` links at confidence `0.80`
or higher. If no strategy entity link exists, approved `STRATEGY` scope rows can
derive strategy-level notes.

Mixed-scope content requires body anchors for portfolio and strategy derivations too.

## Date Logic

`CONTENT_DATES` stores all date candidates.

`APS_CORE.CONTENT_SELECTED_OPERATIONAL_DATES` selects one operational date per
current version for timelines. Version-specific date candidates beat content-level
date candidates before the normal tie-breaker runs.

Tie-breaker:

1. `IS_PRIMARY_CANDIDATE = TRUE`
2. `DATE_PRIORITY_OVERRIDE`, if present
3. Date type priority:
   - `MEETING_DATE`
   - `REPORT_PERIOD_END`
   - `REPORT_PERIOD_START`
   - `EFFECTIVE_DATE_CANDIDATE`
   - `SOURCE_MODIFIED`
   - `SOURCE_CREATED`
   - `INGESTED`
4. Higher confidence
5. Later date
6. Later created timestamp
7. `CONTENT_DATE_ID`

This gives apps one deterministic `EFFECTIVE_DATE` while preserving every candidate.
Undated notes remain valid content and appear with null dates.

## Review Queue

`APS_CORE.CONTENT_REVIEW_ISSUES` emits one row per issue. `APS_MART.CONTENT_REVIEW_QUEUE`
aggregates those issues for app/reviewer workflows.

Review issue codes include:

- `DUPLICATE_CONTENT_ITEM_KEY`
- `DUPLICATE_CONTENT_VERSION_KEY`
- `MULTIPLE_CURRENT_VERSION_FLAGS`
- `MISSING_CURRENT_VERSION_POINTER`
- `MULTIPLE_CURRENT_VERSION_POINTERS`
- `DUPLICATE_VERSION_MARKED_CURRENT`
- `CURRENT_POINTER_FLAG_MISMATCH`
- `MISSING_PRIMARY_BODY`
- `MULTIPLE_PRIMARY_BODIES`
- `MISSING_REVIEWED_ENTITY_LINKS`
- `UNRESOLVED_ENTITY_MENTIONS`
- `SCOPE_CONFLICT`
- `IMAGE_HEAVY_NO_EXTRACTED_CONTEXT`
- `MISSING_INVESTMENT_BODY_ANCHORS`
- `MISSING_PORTFOLIO_BODY_ANCHORS`
- `MISSING_STRATEGY_BODY_ANCHORS`
- `MISSING_OPERATIONAL_DATE`

`MISSING_OPERATIONAL_DATE` is `P3`. It should not block ingestion. It should warn
timeline consumers.

## Data Flow

```text
APS_RAW
  raw files, exports, images, conversion artifacts
    |
    v
adapter staging rows with deterministic keys and fingerprints
    |
    v
APS_CORE.CONTENT_ITEMS
    |
    +--> APS_CORE.CONTENT_VERSIONS
           |
           +--> CONTENT_BODIES
           +--> CONTENT_ASSETS
           +--> CONTENT_SCOPES
           +--> CONTENT_ENTITY_LINKS
           +--> CONTENT_DATES
           |
           v
       CONTENT_ITEM_CURRENT_VERSION
           |
           v
APS_CORE derivation views
    |
    +--> INVESTMENT_NOTES_FROM_CONTENT
    +--> PORTFOLIO_NOTES_FROM_CONTENT
    +--> STRATEGY_NOTES_FROM_CONTENT
    +--> CONTENT_REVIEW_ISSUES
           |
           v
APS_MART timeline and review queue views
```

## Edge Cases Covered

- No investment name: content item ingests; no investment note derives.
- Multiple investments: one item; multiple body-anchored investment derivations.
- Strategy-level notes: strategy/topic derivation or approved strategy scope.
- Portfolio-level notes: portfolio/fund derivation.
- Undated notes: null operational date plus review warning.
- Image-heavy notes: assets are first-class and review queue flags missing context.
- Duplicate/re-exported notes: version keys and duplicate version status prevent
  duplicate current rows.
- Revised notes: same item, new current version.
- Mixed scope notes: body anchors required for derived records.
- Unresolved entity mentions: review issue.
- Multiple valid dates: deterministic selected operational date plus all candidates.

## NOT in Scope

- Full assertion/KPI extraction from arbitrary notes. That can build on anchored
  bodies later.
- Full knowledge graph modeling. Entity links are enough for this ingestion layer.
- Replacing the existing `INVESTMENT_NOTES` table. The compatibility view lets the
  app migrate safely.
- Enforced uniqueness in Snowflake standard tables. Use merge keys, pointer updates,
  and review issues; constraints alone are not enough.

## Recommendation

This v2 is enough to start building the app against generic content, with one caveat:
do not skip adapter discipline.

If adapters produce deterministic keys, body anchors, fingerprints, reviewed entity
links, and date candidates, the model is ready for real ingestion and app-facing
timeline work. If adapters only dump whole markdown blobs with loose entity names,
the schema cannot save the product. The bad data will just be better organized.
