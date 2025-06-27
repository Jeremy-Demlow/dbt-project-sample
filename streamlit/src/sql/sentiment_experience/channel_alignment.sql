WITH daily_channel_sentiment AS (
    SELECT
        DATE_TRUNC('day', interaction_date) AS date,
        interaction_type AS source_type,
        AVG(sentiment_score) AS avg_sentiment
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
    GROUP BY 1, 2
)
SELECT *
FROM daily_channel_sentiment
ORDER BY date, source_type; 