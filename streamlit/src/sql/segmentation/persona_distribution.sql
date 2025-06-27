-- sql/segmentation/persona_distribution.sql
-- Provides data for Persona Distribution chart and Segment Explorer table.

-- Provides Persona and Customer Count for the Persona Distribution chart.
SELECT
    DERIVED_PERSONA AS PERSONA,
    COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMER_COUNT
FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS
WHERE DERIVED_PERSONA IS NOT NULL AND DERIVED_PERSONA != '' -- Ensure persona is not null or empty
GROUP BY 1
ORDER BY 2 DESC; 