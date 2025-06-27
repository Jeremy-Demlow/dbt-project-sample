-- sql/segmentation/churn_vs_upsell.sql
-- Provides data for Churn vs Upsell Density plot.
-- KPI related columns (SEGMENT_ENGAGEMENT_INDEX, etc.) have been removed.

WITH customer_scores AS (
    -- Renamed CTE as engagement index specific parts are removed.
    SELECT
        cps.CUSTOMER_ID,
        CASE LOWER(cps.churn_risk)
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0 -- Default or NULL for unknown values
        END AS CHURN_SCORE,
        CASE LOWER(cps.upsell_opportunity)
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0 -- Default or NULL for unknown values
        END AS UPSELL_POTENTIAL
        -- cps.TICKET_COUNT was previously selected here for an engagement index KPI, now removed.
    FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps
    WHERE cps.churn_risk IS NOT NULL AND cps.upsell_opportunity IS NOT NULL
)
-- Select individual scores for the density plot.
SELECT
    cs.CHURN_SCORE,
    cs.UPSELL_POTENTIAL
FROM
    customer_scores cs
WHERE cs.CHURN_SCORE IS NOT NULL AND cs.UPSELL_POTENTIAL IS NOT NULL; -- Ensure scores are not null for plotting 