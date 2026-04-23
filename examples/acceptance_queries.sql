-- Acceptance queries for the APS generic content model v2.
--
-- Run after loading representative fixtures from the broad notes corpus. These
-- checks intentionally avoid inventing investments or fake dates.

-- 1. PEP Weekly: strategy note with no investment still lands canonically.
SELECT
    ccv.CONTENT_ID,
    ccv.CONTENT_VERSION_ID,
    ccv.SOURCE_TITLE,
    ccv.SOURCE_TYPE,
    ccv.CONTENT_REVIEW_STATUS
FROM APS_CORE.CONTENT_CURRENT_VERSIONS ccv
WHERE ccv.SOURCE_TITLE ILIKE '%PEP Weekly%'
  AND NOT EXISTS (
      SELECT 1
      FROM APS_CORE.CONTENT_ENTITY_LINKS cel
      WHERE cel.CONTENT_ID = ccv.CONTENT_ID
        AND UPPER(cel.ENTITY_TYPE) = 'INVESTMENT'
        AND UPPER(cel.REVIEW_STATUS) IN ('AUTO_ACCEPTED', 'APPROVED')
  );

-- 2. SAIF Portfolio Redemption: portfolio-level note links to portfolio/fund
-- scope without pretending there is an investment.
SELECT
    ccv.CONTENT_ID,
    ccv.SOURCE_TITLE,
    pn.PORTFOLIO_NAME,
    pn.PORTFOLIO_ENTITY_TYPE,
    pn.ENTITY_LINK_CONFIDENCE,
    pn.DERIVATION_STATUS
FROM APS_CORE.CONTENT_CURRENT_VERSIONS ccv
JOIN APS_CORE.PORTFOLIO_NOTES_FROM_CONTENT pn
    ON pn.SOURCE_CONTENT_ID = ccv.CONTENT_ID
WHERE ccv.SOURCE_TITLE ILIKE '%SAIF%Portfolio%Redemption%';

-- 3. Decarb Partners Fund I Q4 2025 Webcast: image-heavy content preserves
-- linked assets plus extracted context when available.
SELECT
    ccv.CONTENT_ID,
    ccv.SOURCE_TITLE,
    COUNT(ca.CONTENT_ASSET_ID) AS ASSET_COUNT,
    COUNT_IF(UPPER(ca.ASSET_TYPE) IN ('IMAGE', 'CHART', 'SCREENSHOT', 'TABLE_IMAGE')) AS IMAGE_LIKE_ASSET_COUNT,
    COUNT_IF(ca.OCR_TEXT IS NOT NULL OR ca.CAPTION_TEXT IS NOT NULL OR ca.CHART_DATA IS NOT NULL) AS ASSETS_WITH_CONTEXT
FROM APS_CORE.CONTENT_CURRENT_VERSIONS ccv
JOIN APS_CORE.CONTENT_ASSETS ca
    ON ca.CONTENT_VERSION_ID = ccv.CONTENT_VERSION_ID
WHERE ccv.SOURCE_TITLE ILIKE '%Decarb Partners Fund I Q4 2025 Webcast%'
GROUP BY ccv.CONTENT_ID, ccv.SOURCE_TITLE;

-- 4. Multi-investment note: one content item can produce multiple body-anchored
-- investment-note rows when each investment link is high confidence.
SELECT
    SOURCE_CONTENT_ID,
    COUNT(DISTINCT SOURCE_ENTITY_LINK_ID) AS DERIVED_INVESTMENT_NOTE_COUNT,
    COUNT_IF(SOURCE_CONTENT_BODY_ID IS NULL) AS DERIVED_ROWS_WITHOUT_BODY_ANCHOR,
    LISTAGG(DISTINCT INVESTMENT_NAME, ', ') WITHIN GROUP (ORDER BY INVESTMENT_NAME) AS INVESTMENTS
FROM APS_CORE.INVESTMENT_NOTES_FROM_CONTENT
GROUP BY SOURCE_CONTENT_ID
HAVING COUNT(DISTINCT SOURCE_ENTITY_LINK_ID) >= 2;

-- 5. Duplicate / re-exported notes: duplicates are identified by version
-- fingerprints and are not marked current.
SELECT
    CONTENT_FINGERPRINT,
    COUNT(DISTINCT CONTENT_VERSION_ID) AS VERSION_COUNT,
    COUNT_IF(UPPER(VERSION_STATUS) = 'DUPLICATE') AS DUPLICATE_VERSION_COUNT,
    COUNT_IF(IS_CURRENT AND UPPER(VERSION_STATUS) = 'DUPLICATE') AS CURRENT_DUPLICATE_VERSION_COUNT
FROM APS_CORE.CONTENT_VERSIONS
WHERE CONTENT_FINGERPRINT IS NOT NULL
GROUP BY CONTENT_FINGERPRINT
HAVING COUNT(DISTINCT CONTENT_VERSION_ID) > 1;

-- 6. Undated notes: content remains queryable without fake operational dates.
SELECT
    ccv.CONTENT_ID,
    ccv.SOURCE_TITLE,
    ccv.SOURCE_TYPE,
    ccv.SOURCE_CREATED_AT,
    ccv.SOURCE_MODIFIED_AT,
    ccv.CURRENT_VERSION_SELECTED_AT
FROM APS_CORE.CONTENT_CURRENT_VERSIONS ccv
LEFT JOIN APS_CORE.CONTENT_SELECTED_OPERATIONAL_DATES sod
    ON sod.CONTENT_ID = ccv.CONTENT_ID
   AND sod.CONTENT_VERSION_ID = ccv.CONTENT_VERSION_ID
WHERE sod.CONTENT_DATE_ID IS NULL;

-- 7. Conservative derivation: ambiguous investment links are held for review and
-- do not appear in the derived investment-note timeline.
SELECT
    cel.CONTENT_ID,
    cel.ENTITY_NAME,
    cel.CONFIDENCE,
    cel.REVIEW_STATUS
FROM APS_CORE.CONTENT_ENTITY_LINKS cel
LEFT JOIN APS_CORE.INVESTMENT_NOTES_FROM_CONTENT inv
    ON inv.SOURCE_ENTITY_LINK_ID = cel.CONTENT_ENTITY_LINK_ID
WHERE UPPER(cel.ENTITY_TYPE) = 'INVESTMENT'
  AND (
      COALESCE(cel.CONFIDENCE, 0) < 0.85
      OR UPPER(cel.REVIEW_STATUS) NOT IN ('AUTO_ACCEPTED', 'APPROVED')
  )
  AND inv.SOURCE_ENTITY_LINK_ID IS NULL;

-- 8. Revised notes: every active content item should have exactly one current
-- version pointer.
SELECT
    ci.CONTENT_ID,
    ci.SOURCE_TITLE,
    COUNT(ccv.CURRENT_CONTENT_VERSION_ID) AS CURRENT_POINTER_COUNT
FROM APS_CORE.CONTENT_ITEMS ci
LEFT JOIN APS_CORE.CONTENT_ITEM_CURRENT_VERSION ccv
    ON ccv.CONTENT_ID = ci.CONTENT_ID
WHERE ci.IS_ACTIVE
GROUP BY ci.CONTENT_ID, ci.SOURCE_TITLE
HAVING COUNT(ccv.CURRENT_CONTENT_VERSION_ID) <> 1;

-- 9. Idempotency: duplicate content item/version keys are P1 review issues.
SELECT
    ISSUE_CODE,
    COUNT(*) AS ISSUE_COUNT
FROM APS_CORE.CONTENT_REVIEW_ISSUES
WHERE ISSUE_CODE IN ('DUPLICATE_CONTENT_ITEM_KEY', 'DUPLICATE_CONTENT_VERSION_KEY')
GROUP BY ISSUE_CODE;

-- 10. Mixed-scope notes: high-confidence investment links without body anchors
-- should appear in the review issue view, not in derived investment notes.
SELECT
    cri.CONTENT_ID,
    cri.CONTENT_VERSION_ID,
    cri.ISSUE_CODE,
    cri.SEVERITY,
    cri.ISSUE_CONTEXT
FROM APS_CORE.CONTENT_REVIEW_ISSUES cri
WHERE cri.ISSUE_CODE IN (
    'MISSING_INVESTMENT_BODY_ANCHORS',
    'MISSING_PORTFOLIO_BODY_ANCHORS',
    'MISSING_STRATEGY_BODY_ANCHORS',
    'SCOPE_CONFLICT'
);

-- 11. Multiple valid dates: the selected operational date is deterministic and
-- traceable back to one candidate.
SELECT
    ccv.CONTENT_ID,
    ccv.SOURCE_TITLE,
    sod.CONTENT_DATE_ID,
    sod.OPERATIONAL_DATE_TYPE,
    sod.OPERATIONAL_DATE,
    sod.DATE_SELECTION_PRIORITY,
    sod.OPERATIONAL_DATE_CONFIDENCE
FROM APS_CORE.CONTENT_CURRENT_VERSIONS ccv
JOIN APS_CORE.CONTENT_SELECTED_OPERATIONAL_DATES sod
    ON sod.CONTENT_ID = ccv.CONTENT_ID
   AND sod.CONTENT_VERSION_ID = ccv.CONTENT_VERSION_ID;
