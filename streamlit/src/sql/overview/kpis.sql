-- Overview KPIs
WITH combined_metrics AS (
    SELECT
        i.interaction_id,
        i.sentiment_score,
        cps.persona,
        cps.churn_risk,
        r.review_rating
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS i
    LEFT JOIN ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps ON i.customer_id = cps.customer_id
    LEFT JOIN ANALYTICS.FACT_PRODUCT_REVIEWS r ON i.customer_id = r.customer_id
)

SELECT
    AVG(sentiment_score) as avg_sentiment,
    COUNT(DISTINCT interaction_id) as total_interactions,
    (COUNT_IF(churn_risk = 'High') / COUNT(*)) * 100 as high_risk_pct,
    AVG(review_rating) as avg_rating
FROM combined_metrics; 