WITH base_date AS (
    SELECT MIN(interaction_date) as min_date
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
)
SELECT
    cps.persona,
    CAST(AVG(s.sentiment_score) as FLOAT) as avg_sentiment,
    CAST(STDDEV(s.sentiment_score) as FLOAT) as sentiment_volatility,
    CAST(REGR_SLOPE(s.sentiment_score, DATEDIFF('day', base_date.min_date, s.interaction_date)) as FLOAT) as sentiment_trend,
    COUNT(*) as interaction_count
FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS s
JOIN ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps ON s.customer_id = cps.customer_id
CROSS JOIN base_date
GROUP BY 1
ORDER BY 1; 