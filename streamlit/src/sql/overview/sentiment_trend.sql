-- Daily sentiment trend with moving average
WITH daily_sentiment AS (
    SELECT
        DATE_TRUNC('day', i.interaction_date) as date,
        CAST(AVG(i.sentiment_score) as FLOAT) as avg_sentiment
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS i
    GROUP BY DATE_TRUNC('day', i.interaction_date)
    ORDER BY DATE_TRUNC('day', i.interaction_date)
)

SELECT
    date as DATE,
    avg_sentiment as AVG_SENTIMENT,
    CAST(AVG(avg_sentiment) OVER (
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as FLOAT) as MOVING_AVG_SENTIMENT
FROM daily_sentiment
WHERE avg_sentiment IS NOT NULL
ORDER BY date; 