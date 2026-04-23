-- Acceptance queries for the APS generic content model.
--
-- These are scenario checks to run after loading representative fixtures from the
-- broad notes corpus. They intentionally avoid inventing investments or dates.

-- 1. PEP Weekly: strategy note with no investment still lands canonically.
SELECT
    CONTENT_ID,
    SOURCE_TITLE,
    SOURCE_TYPE,
    REVIEW_STATUS
FROM APS_CORE.CONTENT_ITEMS
WHERE SOURCE_TITLE ILIKE '%PEP Weekly%'
  AND CONTENT_ID NOT IN (
      SELECT CONTENT_ID
      FROM APS_CORE.CONTENT_ENTITY_LINKS
      WHERE UPPER(ENTITY_TYPE) = 'INVESTMENT'
        AND UPPER(REVIEW_STATUS) IN ('AUTO_ACCEPTED', 'APPROVED')
  );

-- 2. SAIF Portfolio Redemption: portfolio-level note links to portfolio/fund
-- scope without pretending there is an investment.
SELECT
    ci.CONTENT_ID,
    ci.SOURCE_TITLE,
    cs.SCOPE_TYPE,
    cel.ENTITY_TYPE,
    cel.ENTITY_NAME,
    cel.CONFIDENCE
FROM APS_CORE.CONTENT_ITEMS ci
JOIN APS_CORE.CONTENT_SCOPES cs
    ON cs.CONTENT_ID = ci.CONTENT_ID
LEFT JOIN APS_CORE.CONTENT_ENTITY_LINKS cel
    ON cel.CONTENT_ID = ci.CONTENT_ID
WHERE ci.SOURCE_TITLE ILIKE '%SAIF%Portfolio%Redemption%'
  AND UPPER(cs.SCOPE_TYPE) = 'PORTFOLIO'
  AND UPPER(cel.ENTITY_TYPE) IN ('PORTFOLIO', 'FUND');

-- 3. Decarb Partners Fund I Q4 2025 Webcast: image-heavy content preserves
-- linked assets plus extracted context when available.
SELECT
    ci.CONTENT_ID,
    ci.SOURCE_TITLE,
    COUNT(*) AS ASSET_COUNT,
    COUNT_IF(ca.OCR_TEXT IS NOT NULL OR ca.CAPTION_TEXT IS NOT NULL OR ca.CHART_DATA IS NOT NULL) AS ASSETS_WITH_CONTEXT
FROM APS_CORE.CONTENT_ITEMS ci
JOIN APS_CORE.CONTENT_VERSIONS cv
    ON cv.CONTENT_ID = ci.CONTENT_ID
JOIN APS_CORE.CONTENT_ASSETS ca
    ON ca.CONTENT_VERSION_ID = cv.CONTENT_VERSION_ID
WHERE ci.SOURCE_TITLE ILIKE '%Decarb Partners Fund I Q4 2025 Webcast%'
GROUP BY ci.CONTENT_ID, ci.SOURCE_TITLE;

-- 4. Multi-investment note: one content item can produce multiple conservative
-- investment-note rows when each investment link is high confidence.
SELECT
    SOURCE_CONTENT_ID,
    COUNT(DISTINCT SOURCE_ENTITY_LINK_ID) AS DERIVED_INVESTMENT_NOTE_COUNT,
    LISTAGG(DISTINCT INVESTMENT_NAME, ', ') WITHIN GROUP (ORDER BY INVESTMENT_NAME) AS INVESTMENTS
FROM APS_CORE.INVESTMENT_NOTES_FROM_CONTENT
GROUP BY SOURCE_CONTENT_ID
HAVING COUNT(DISTINCT SOURCE_ENTITY_LINK_ID) >= 2;

-- 5. Duplicate / re-exported notes: duplicates are identified by content
-- fingerprints, not URL alone.
SELECT
    CONTENT_FINGERPRINT,
    COUNT(DISTINCT CONTENT_VERSION_ID) AS VERSION_COUNT,
    COUNT(DISTINCT CONTENT_ID) AS CONTENT_ITEM_COUNT,
    COUNT_IF(DUPLICATE_OF_CONTENT_VERSION_ID IS NOT NULL) AS DUPLICATE_VERSION_COUNT
FROM APS_CORE.CONTENT_VERSIONS
WHERE CONTENT_FINGERPRINT IS NOT NULL
GROUP BY CONTENT_FINGERPRINT
HAVING COUNT(DISTINCT CONTENT_VERSION_ID) > 1;

-- 6. Undated notes: content remains queryable without fake effective dates.
SELECT
    ci.CONTENT_ID,
    ci.SOURCE_TITLE,
    ci.SOURCE_TYPE,
    ci.SOURCE_CREATED_AT,
    ci.SOURCE_MODIFIED_AT,
    ci.INGESTED_AT
FROM APS_CORE.CONTENT_ITEMS ci
WHERE ci.CONTENT_ID NOT IN (
    SELECT CONTENT_ID
    FROM APS_CORE.CONTENT_DATES
    WHERE UPPER(DATE_TYPE) IN ('MEETING_DATE', 'REPORT_PERIOD_END', 'EFFECTIVE_DATE_CANDIDATE')
)
AND ci.IS_ACTIVE;

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

