-- Daily risk percentage trend
WITH daily_risk AS (
    SELECT
        DATE_TRUNC('day', i.interaction_date) as date,
        COUNT_IF(i.sentiment_score < 0.3) as high_risk_count,
        COUNT(*) as total_count
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS i
    GROUP BY 1
)

SELECT
    date as DATE,
    CAST((high_risk_count * 100.0 / total_count) as FLOAT) as HIGH_RISK_PCT
FROM daily_risk
ORDER BY date; 