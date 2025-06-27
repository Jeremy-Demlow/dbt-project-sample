-- Recent Reviews with Sentiment Analysis
SELECT
    review_id,
    review_date,
    review_text,
    review_text_english,
    review_language,
    review_rating,
    sentiment_score
FROM ANALYTICS.FACT_PRODUCT_REVIEWS
ORDER BY review_date DESC 