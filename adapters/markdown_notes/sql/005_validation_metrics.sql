-- Validation metrics for the APS markdown-note pilot.
--
-- Expected session input:
--   SET PILOT_INGESTION_RUN_ID = '<ingestion-run-id>';
--
-- Run each section independently after the pilot load finishes. The harness can
-- consume these result sets directly or export them to JSON/CSV for
-- validation_report.py.

-- ---------------------------------------------------------------------------
-- 1. Pilot current-version scope
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT *
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    COUNT(DISTINCT CONTENT_ID) AS PILOT_CONTENT_ITEM_COUNT,
    COUNT(DISTINCT CONTENT_VERSION_ID) AS PILOT_CURRENT_VERSION_COUNT
FROM pilot_current_versions;

-- ---------------------------------------------------------------------------
-- 2. Required review-issue counts
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_VERSION_ID
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    ISSUE_CODE,
    COUNT(*) AS ISSUE_COUNT
FROM APS_CORE.CONTENT_REVIEW_ISSUES cri
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = cri.CONTENT_ID
 AND (cri.CONTENT_VERSION_ID = pcv.CONTENT_VERSION_ID OR cri.CONTENT_VERSION_ID IS NULL)
WHERE ISSUE_CODE IN (
    'DUPLICATE_CONTENT_ITEM_KEY',
    'DUPLICATE_CONTENT_VERSION_KEY',
    'MISSING_PRIMARY_BODY',
    'UNRESOLVED_ENTITY_MENTIONS',
    'MISSING_INVESTMENT_BODY_ANCHORS',
    'MISSING_PORTFOLIO_BODY_ANCHORS',
    'MISSING_STRATEGY_BODY_ANCHORS',
    'MISSING_OPERATIONAL_DATE'
)
GROUP BY ISSUE_CODE
ORDER BY ISSUE_CODE;

-- ---------------------------------------------------------------------------
-- 3. Missing primary body, unresolved mentions, missing anchors, and missing
--    operational dates with note-level detail
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_VERSION_ID, SOURCE_TITLE
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    pcv.SOURCE_TITLE,
    cri.CONTENT_ID,
    cri.CONTENT_VERSION_ID,
    cri.ISSUE_CODE,
    cri.SEVERITY,
    cri.ISSUE_DETAIL,
    cri.ISSUE_CONTEXT
FROM APS_CORE.CONTENT_REVIEW_ISSUES cri
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = cri.CONTENT_ID
 AND (cri.CONTENT_VERSION_ID = pcv.CONTENT_VERSION_ID OR cri.CONTENT_VERSION_ID IS NULL)
WHERE cri.ISSUE_CODE IN (
    'MISSING_PRIMARY_BODY',
    'UNRESOLVED_ENTITY_MENTIONS',
    'MISSING_INVESTMENT_BODY_ANCHORS',
    'MISSING_PORTFOLIO_BODY_ANCHORS',
    'MISSING_STRATEGY_BODY_ANCHORS',
    'MISSING_OPERATIONAL_DATE'
)
ORDER BY cri.SEVERITY, pcv.SOURCE_TITLE, cri.ISSUE_CODE;

-- ---------------------------------------------------------------------------
-- 4. Notes blocked from app promotion by P1 issues
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    rq.CONTENT_ID,
    rq.SOURCE_TITLE,
    rq.REVIEW_REASONS,
    rq.P1_ISSUE_COUNT,
    rq.ISSUE_COUNT
FROM APS_MART.CONTENT_REVIEW_QUEUE rq
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = rq.CONTENT_ID
WHERE rq.P1_ISSUE_COUNT > 0
ORDER BY rq.P1_ISSUE_COUNT DESC, rq.SOURCE_TITLE;

-- ---------------------------------------------------------------------------
-- 5. Derived investment/portfolio/strategy note counts
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_VERSION_ID
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT 'investment' AS NOTE_TYPE, COUNT(*) AS DERIVED_NOTE_COUNT
FROM APS_CORE.INVESTMENT_NOTES_FROM_CONTENT inv
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = inv.SOURCE_CONTENT_ID
 AND pcv.CONTENT_VERSION_ID = inv.SOURCE_CONTENT_VERSION_ID
UNION ALL
SELECT 'portfolio' AS NOTE_TYPE, COUNT(*) AS DERIVED_NOTE_COUNT
FROM APS_CORE.PORTFOLIO_NOTES_FROM_CONTENT port
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = port.SOURCE_CONTENT_ID
 AND pcv.CONTENT_VERSION_ID = port.SOURCE_CONTENT_VERSION_ID
UNION ALL
SELECT 'strategy' AS NOTE_TYPE, COUNT(*) AS DERIVED_NOTE_COUNT
FROM APS_CORE.STRATEGY_NOTES_FROM_CONTENT strat
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = strat.SOURCE_CONTENT_ID
 AND pcv.CONTENT_VERSION_ID = strat.SOURCE_CONTENT_VERSION_ID
ORDER BY NOTE_TYPE;

-- ---------------------------------------------------------------------------
-- 6. Unresolved entity mention detail
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_VERSION_ID, SOURCE_TITLE
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    pcv.SOURCE_TITLE,
    cel.CONTENT_ID,
    cel.CONTENT_VERSION_ID,
    cel.ENTITY_TYPE,
    cel.ENTITY_NAME,
    cel.MENTION_TEXT,
    cel.MENTION_CONTEXT,
    cel.CONFIDENCE,
    cel.REVIEW_STATUS,
    cel.CONTENT_BODY_ID
FROM APS_CORE.CONTENT_ENTITY_LINKS cel
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = cel.CONTENT_ID
 AND (cel.CONTENT_VERSION_ID = pcv.CONTENT_VERSION_ID OR cel.CONTENT_VERSION_ID IS NULL)
WHERE cel.ENTITY_ID IS NULL
  AND cel.ENTITY_NAME IS NOT NULL
ORDER BY pcv.SOURCE_TITLE, cel.ENTITY_TYPE, cel.ENTITY_NAME;

-- ---------------------------------------------------------------------------
-- 7. Missing-body-anchor detail for high-confidence approved links
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_VERSION_ID, SOURCE_TITLE
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    pcv.SOURCE_TITLE,
    cel.CONTENT_ID,
    cel.CONTENT_VERSION_ID,
    cel.ENTITY_TYPE,
    cel.ENTITY_ID,
    cel.ENTITY_NAME,
    cel.CONFIDENCE,
    cel.REVIEW_STATUS,
    cel.LINK_ROLE,
    cel.CONTENT_BODY_ID
FROM APS_CORE.CONTENT_ENTITY_LINKS cel
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ID = cel.CONTENT_ID
 AND (cel.CONTENT_VERSION_ID = pcv.CONTENT_VERSION_ID OR cel.CONTENT_VERSION_ID IS NULL)
WHERE cel.CONTENT_BODY_ID IS NULL
  AND (
      (UPPER(cel.ENTITY_TYPE) = 'INVESTMENT' AND COALESCE(cel.CONFIDENCE, 0) >= 0.85)
      OR (UPPER(cel.ENTITY_TYPE) IN ('PORTFOLIO', 'FUND', 'STRATEGY', 'TOPIC') AND COALESCE(cel.CONFIDENCE, 0) >= 0.80)
  )
  AND UPPER(cel.REVIEW_STATUS) IN ('AUTO_ACCEPTED', 'APPROVED')
ORDER BY pcv.SOURCE_TITLE, cel.ENTITY_TYPE, cel.ENTITY_NAME;

-- ---------------------------------------------------------------------------
-- 8. Missing operational date detail
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_VERSION_ID, SOURCE_TITLE
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
)
SELECT
    pcv.SOURCE_TITLE,
    pcv.CONTENT_ID,
    pcv.CONTENT_VERSION_ID
FROM pilot_current_versions pcv
LEFT JOIN APS_CORE.CONTENT_SELECTED_OPERATIONAL_DATES sod
  ON sod.CONTENT_ID = pcv.CONTENT_ID
 AND sod.CONTENT_VERSION_ID = pcv.CONTENT_VERSION_ID
WHERE sod.CONTENT_DATE_ID IS NULL
ORDER BY pcv.SOURCE_TITLE;

-- ---------------------------------------------------------------------------
-- 9. False-positive / false-negative entity examples
--
-- This section requires a labeled expectation set from the pilot manifest.
-- Load the labeled subset into a temp table or replace the CTE below.
-- Suggested columns:
--   CONTENT_ITEM_KEY, ENTITY_TYPE, ENTITY_NAME
-- ---------------------------------------------------------------------------
WITH pilot_current_versions AS (
    SELECT CONTENT_ID, CONTENT_ITEM_KEY, CONTENT_VERSION_ID, SOURCE_TITLE
    FROM APS_CORE.CONTENT_CURRENT_VERSIONS
    WHERE INGESTION_RUN_ID = $PILOT_INGESTION_RUN_ID
),
pilot_expected_entities AS (
    SELECT
        CAST(NULL AS VARCHAR) AS CONTENT_ITEM_KEY,
        CAST(NULL AS VARCHAR) AS ENTITY_TYPE,
        CAST(NULL AS VARCHAR) AS ENTITY_NAME
    WHERE 1 = 0
),
actual_entities AS (
    SELECT DISTINCT
        pcv.CONTENT_ITEM_KEY,
        pcv.SOURCE_TITLE,
        UPPER(cel.ENTITY_TYPE) AS ENTITY_TYPE,
        LOWER(cel.ENTITY_NAME) AS ENTITY_NAME,
        cel.ENTITY_ID,
        cel.REVIEW_STATUS,
        cel.CONFIDENCE
    FROM APS_CORE.CONTENT_ENTITY_LINKS cel
    JOIN pilot_current_versions pcv
      ON pcv.CONTENT_ID = cel.CONTENT_ID
     AND (cel.CONTENT_VERSION_ID = pcv.CONTENT_VERSION_ID OR cel.CONTENT_VERSION_ID IS NULL)
    WHERE cel.ENTITY_NAME IS NOT NULL
      AND cel.ENTITY_ID IS NOT NULL
)
SELECT
    'false_positive' AS EXAMPLE_TYPE,
    ae.SOURCE_TITLE,
    ae.CONTENT_ITEM_KEY,
    ae.ENTITY_TYPE,
    ae.ENTITY_NAME,
    ae.ENTITY_ID,
    ae.REVIEW_STATUS,
    ae.CONFIDENCE
FROM actual_entities ae
LEFT JOIN pilot_expected_entities pee
  ON pee.CONTENT_ITEM_KEY = ae.CONTENT_ITEM_KEY
 AND UPPER(pee.ENTITY_TYPE) = ae.ENTITY_TYPE
 AND LOWER(pee.ENTITY_NAME) = ae.ENTITY_NAME
WHERE pee.CONTENT_ITEM_KEY IS NULL

UNION ALL

SELECT
    'false_negative' AS EXAMPLE_TYPE,
    pcv.SOURCE_TITLE,
    pee.CONTENT_ITEM_KEY,
    UPPER(pee.ENTITY_TYPE) AS ENTITY_TYPE,
    LOWER(pee.ENTITY_NAME) AS ENTITY_NAME,
    NULL AS ENTITY_ID,
    NULL AS REVIEW_STATUS,
    NULL AS CONFIDENCE
FROM pilot_expected_entities pee
JOIN pilot_current_versions pcv
  ON pcv.CONTENT_ITEM_KEY = pee.CONTENT_ITEM_KEY
LEFT JOIN actual_entities ae
  ON ae.CONTENT_ITEM_KEY = pee.CONTENT_ITEM_KEY
 AND ae.ENTITY_TYPE = UPPER(pee.ENTITY_TYPE)
 AND ae.ENTITY_NAME = LOWER(pee.ENTITY_NAME)
WHERE ae.CONTENT_ITEM_KEY IS NULL
ORDER BY EXAMPLE_TYPE, SOURCE_TITLE, ENTITY_TYPE, ENTITY_NAME;

