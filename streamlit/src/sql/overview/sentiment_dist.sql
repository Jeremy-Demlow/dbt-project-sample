-- Sentiment score distribution
WITH sentiment_buckets AS (
    SELECT
        CASE
            WHEN sentiment_score < 0.2 THEN 'Very Negative'
            WHEN sentiment_score < 0.4 THEN 'Negative'
            WHEN sentiment_score < 0.6 THEN 'Neutral'
            WHEN sentiment_score < 0.8 THEN 'Positive'
            ELSE 'Very Positive'
        END as sentiment_bucket,
        COUNT(*) as count
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS i
    LEFT JOIN ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps ON i.customer_id = cps.customer_id
    GROUP BY 1
)

SELECT
    sentiment_bucket as SENTIMENT_BUCKET,
    count as COUNT,
    CAST((count * 1.0 / SUM(count) OVER ()) as FLOAT) as PERCENTAGE
FROM sentiment_buckets
ORDER BY 
    CASE sentiment_bucket
        WHEN 'Very Negative' THEN 1
        WHEN 'Negative' THEN 2
        WHEN 'Neutral' THEN 3
        WHEN 'Positive' THEN 4
        WHEN 'Very Positive' THEN 5
    END; 