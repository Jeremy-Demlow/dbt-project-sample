SELECT
    DATE_TRUNC('day', interaction_date) AS date,
    sentiment_score,
    COUNT(*) AS count,
    interaction_type AS source_type
FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
GROUP BY 1, 2, 4
ORDER BY 1, 2, 4; 