-- Sentiment Analysis by Language
SELECT
    DATE_TRUNC('day', review_date) as review_date,
    review_language,
    CAST(AVG(sentiment_score) as FLOAT) as avg_sentiment,
    COUNT(*) as review_count
FROM ANALYTICS.FACT_PRODUCT_REVIEWS
GROUP BY 1, 2
ORDER BY 1, 2; 