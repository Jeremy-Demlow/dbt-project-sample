-- Sentiment trends model that analyzes sentiment patterns over time
-- This model calculates sentiment trends, volatility, and historical patterns
-- Uses window functions to track changes in sentiment over time

WITH sentiment_data AS (
    SELECT
        customer_id,
        sentiment_score,
        interaction_date,
        FIRST_VALUE(sentiment_score) OVER (PARTITION BY customer_id ORDER BY interaction_date) AS first_sentiment,
        LAST_VALUE(sentiment_score) OVER (PARTITION BY customer_id ORDER BY interaction_date 
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sentiment
    FROM {{ ref('sentiment_analysis') }}
)
SELECT
    customer_id,
    ARRAY_AGG(sentiment_score) WITHIN GROUP (ORDER BY interaction_date) AS sentiment_history,
    AVG(sentiment_score) AS avg_sentiment,
    MIN(sentiment_score) AS min_sentiment,
    MAX(sentiment_score) AS max_sentiment,
    MAX(sentiment_score) - MIN(sentiment_score) AS sentiment_volatility,
    CASE
        WHEN COUNT(*) > 1 THEN 
            MAX(last_sentiment) - MAX(first_sentiment)
        ELSE 0
    END AS sentiment_trend
FROM sentiment_data
GROUP BY customer_id 