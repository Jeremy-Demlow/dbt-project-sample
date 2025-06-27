SELECT
    DATE_TRUNC('day', interaction_date) AS date,
    interaction_type AS source_type,
    COUNT(*) AS interaction_count,
    AVG(sentiment_score) AS avg_sentiment,
    AVG(AVG(sentiment_score)) OVER (
        ORDER BY DATE_TRUNC('day', interaction_date)
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg
FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
GROUP BY 1, 2
ORDER BY 1, 2; 