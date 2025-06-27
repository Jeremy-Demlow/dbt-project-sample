SELECT
    DATE_TRUNC('day', interaction_date) AS date,
    AVG(sentiment_score) AS avg_sentiment
FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
GROUP BY 1
ORDER BY 1; 