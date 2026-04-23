# APS Content Ingestion Contract v2

## Purpose

Adapters convert broad note-like sources into deterministic APS_CORE rows.

The schema is intentionally generic, but ingestion cannot be casual. Real safety
comes from stable keys, fingerprinting, current-version updates, and reviewable
entity/date evidence.

## Required Adapter Output

Every adapter must produce:

- One `CONTENT_ITEMS` candidate row per logical source object.
- One `CONTENT_VERSIONS` candidate row per observed normalized version.
- At least one `CONTENT_BODIES` row for the primary document body.
- Zero or more section `CONTENT_BODIES` rows when entity-specific sections exist.
- Zero or more `CONTENT_ASSETS` rows for images, charts, screenshots, and
  attachments.
- Zero or more `CONTENT_SCOPES` rows.
- Zero or more `CONTENT_ENTITY_LINKS` rows.
- Zero or more `CONTENT_DATES` rows.

Do not drop a note because it has no investment or no date.

## Deterministic Keys

Recommended key formulas:

```text
CONTENT_ITEM_KEY =
  SOURCE_SYSTEM || ':' ||
  COALESCE(SOURCE_OBJECT_ID, NORMALIZED_SOURCE_URI, SOURCE_CONTAINER || '/' || NORMALIZED_SOURCE_PATH)

CONTENT_VERSION_KEY =
  CONTENT_ITEM_KEY || ':' ||
  COALESCE(SOURCE_VERSION_ID, CONTENT_FINGERPRINT, SOURCE_FILE_HASH)

CONTENT_BODY_KEY =
  CONTENT_VERSION_KEY || ':body:' || BODY_ROLE || ':' ||
  COALESCE(SECTION_PATH, 'document') || ':' ||
  COALESCE(NORMALIZED_BODY_HASH, BODY_HASH)

CONTENT_ASSET_KEY =
  CONTENT_VERSION_KEY || ':asset:' ||
  COALESCE(SOURCE_ASSET_URI, FILE_NAME, ASSET_ORDER::VARCHAR) || ':' ||
  COALESCE(ASSET_HASH, 'no_hash')

CONTENT_ENTITY_LINK_KEY =
  CONTENT_VERSION_KEY || ':entity:' || ENTITY_TYPE || ':' ||
  COALESCE(ENTITY_ID, NORMALIZE(ENTITY_NAME)) || ':' ||
  COALESCE(CONTENT_BODY_ID, CONTENT_ASSET_ID, 'document') || ':' ||
  COALESCE(MENTION_START::VARCHAR, 'na') || '-' || COALESCE(MENTION_END::VARCHAR, 'na')

CONTENT_DATE_KEY =
  CONTENT_VERSION_KEY || ':date:' || DATE_TYPE || ':' ||
  COALESCE(DATE_VALUE::VARCHAR, TIMESTAMP_VALUE::VARCHAR, PERIOD_START_DATE::VARCHAR || '/' || PERIOD_END_DATE::VARCHAR, DATE_TEXT)
```

Use project-standard normalization for whitespace, case, URL decoding, and path
separator cleanup before hashing.

## Merge Order

```text
1. Normalize adapter output into staging rows.
2. MERGE CONTENT_ITEMS by CONTENT_ITEM_KEY.
3. MERGE CONTENT_VERSIONS by CONTENT_VERSION_KEY.
4. If the version is a new current version:
   a. Mark old current version rows for the item as SUPERSEDED and IS_CURRENT = FALSE.
   b. Mark the new version as CURRENT and IS_CURRENT = TRUE.
   c. MERGE CONTENT_ITEM_CURRENT_VERSION for the item.
5. MERGE CONTENT_BODIES by CONTENT_BODY_KEY.
6. MERGE CONTENT_ASSETS by CONTENT_ASSET_KEY.
7. MERGE CONTENT_SCOPES by CONTENT_SCOPE_KEY.
8. MERGE CONTENT_ENTITY_LINKS by CONTENT_ENTITY_LINK_KEY.
9. MERGE CONTENT_DATES by CONTENT_DATE_KEY.
10. Query CONTENT_REVIEW_ISSUES and quarantine P1 records from app promotion.
```

Run steps 2 through 9 in one transaction per adapter batch when possible.

## Item vs Version Decision Tree

```text
Incoming source object
    |
    +-- Stable source object id exists?
    |      |
    |      +-- yes --> CONTENT_ITEM_KEY = source-system + source-object-id
    |      |
    |      +-- no --> use normalized URI/container/path as item key
    |
    v
Existing CONTENT_ITEM_KEY?
    |
    +-- no --> insert new CONTENT_ITEM and initial CONTENT_VERSION
    |
    +-- yes --> compare CONTENT_VERSION_KEY / CONTENT_FINGERPRINT
             |
             +-- same fingerprint --> re-export or moved file, update item metadata, no new current version
             |
             +-- different fingerprint --> new CONTENT_VERSION, promote to current
```

## Re-Exports and Moved Files

Re-export with same fingerprint:

- Same `CONTENT_ITEM`.
- Do not create a second current version.
- Optional: insert a `CONTENT_VERSION` with `VERSION_STATUS = 'DUPLICATE'` and
  `DUPLICATE_OF_CONTENT_VERSION_ID` pointing to the original version.

Moved file with stable source object id:

- Same `CONTENT_ITEM`.
- Update `SOURCE_PATH`, `SOURCE_URI`, `SOURCE_URI_NORMALIZED`, and `LAST_SEEN_AT`.
- No new current version if fingerprint is unchanged.

Moved file without stable source object id:

- Use fingerprint reconciliation.
- If confidence is high, keep one canonical item and mark the other
  `DUPLICATE_OF_CONTENT_ID`.
- If confidence is not high, create a new item and route both through review.

Revised note:

- Same `CONTENT_ITEM`.
- New `CONTENT_VERSION`.
- Old current version becomes `SUPERSEDED`.
- `CONTENT_ITEM_CURRENT_VERSION` points to the revised version.

## Entity Link Anchoring

Adapters should create section bodies before entity links.

Rules:

- If an entity is the document-level subject, use `LINK_ROLE = 'DOCUMENT_SUBJECT'`
  or `PRIMARY_SUBJECT`.
- If an entity applies only to a section, anchor `CONTENT_ENTITY_LINKS.CONTENT_BODY_ID`
  to that section body.
- If an entity is only mentioned, set `LINK_ROLE = 'MENTION'` and include mention
  offsets when available.
- For mixed-scope and multi-investment notes, app derivations require body anchors.

## Date Candidates

Adapters should emit all meaningful date candidates:

- `MEETING_DATE`
- `REPORT_PERIOD_END`
- `REPORT_PERIOD_START`
- `EFFECTIVE_DATE_CANDIDATE`
- `SOURCE_MODIFIED`
- `SOURCE_CREATED`
- `INGESTED`

Set `IS_PRIMARY_CANDIDATE` only when the adapter has strong evidence. Do not set it
just because a date is convenient.

Use `DATE_PRIORITY_OVERRIDE` sparingly. It exists for source-specific evidence, not
for hiding uncertainty.

## P1 Quarantine Rules

Records with these review issues should not be promoted to app timelines until fixed:

- `DUPLICATE_CONTENT_ITEM_KEY`
- `DUPLICATE_CONTENT_VERSION_KEY`
- `MULTIPLE_CURRENT_VERSION_FLAGS`
- `MISSING_CURRENT_VERSION_POINTER`
- `MULTIPLE_CURRENT_VERSION_POINTERS`
- `DUPLICATE_VERSION_MARKED_CURRENT`
- `MISSING_PRIMARY_BODY`
- `MISSING_INVESTMENT_BODY_ANCHORS`

P2 records can be ingested but should show up in reviewer workflows before broad use.

P3 records are informational. Example: undated content with no selected operational
date.

## Snowflake Merge Sketch

```sql
MERGE INTO APS_CORE.CONTENT_ITEMS target
USING STAGE_CONTENT_ITEMS source
ON target.CONTENT_ITEM_KEY = source.CONTENT_ITEM_KEY
WHEN MATCHED THEN UPDATE SET
    SOURCE_URI = source.SOURCE_URI,
    SOURCE_URI_NORMALIZED = source.SOURCE_URI_NORMALIZED,
    SOURCE_PATH = source.SOURCE_PATH,
    SOURCE_TITLE = source.SOURCE_TITLE,
    SOURCE_MODIFIED_AT = source.SOURCE_MODIFIED_AT,
    LAST_SEEN_AT = CURRENT_TIMESTAMP(),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    CONTENT_ID,
    CONTENT_ITEM_KEY,
    LOGICAL_SOURCE_KEY,
    ITEM_IDENTITY_METHOD,
    SOURCE_SYSTEM,
    SOURCE_OBJECT_ID,
    SOURCE_CONTAINER,
    SOURCE_PATH,
    SOURCE_URI,
    SOURCE_URI_NORMALIZED,
    SOURCE_REFERENCE,
    SOURCE_TYPE,
    SOURCE_SUBTYPE,
    SOURCE_TITLE,
    SOURCE_PROVENANCE,
    SOURCE_CREATED_AT,
    SOURCE_MODIFIED_AT,
    CANONICAL_CONTENT_HASH
) VALUES (
    source.CONTENT_ID,
    source.CONTENT_ITEM_KEY,
    source.LOGICAL_SOURCE_KEY,
    source.ITEM_IDENTITY_METHOD,
    source.SOURCE_SYSTEM,
    source.SOURCE_OBJECT_ID,
    source.SOURCE_CONTAINER,
    source.SOURCE_PATH,
    source.SOURCE_URI,
    source.SOURCE_URI_NORMALIZED,
    source.SOURCE_REFERENCE,
    source.SOURCE_TYPE,
    source.SOURCE_SUBTYPE,
    source.SOURCE_TITLE,
    source.SOURCE_PROVENANCE,
    source.SOURCE_CREATED_AT,
    source.SOURCE_MODIFIED_AT,
    source.CANONICAL_CONTENT_HASH
);
```

This is a sketch, not the full loader. The loader must also promote exactly one
current version and update `CONTENT_ITEM_CURRENT_VERSION`.
