-- Fact model for product reviews with sentiment analysis and translations
-- This model enriches product reviews with AI-generated sentiment scores and translations
-- Uses Snowflake Cortex for sentiment analysis and translation

SELECT
    r.review_id,
    r.customer_id,
    r.product_id,
    r.review_date,
    r.review_rating,
    r.review_text,
    r.review_language,
    -- Add sentiment analysis
    SNOWFLAKE.CORTEX.SENTIMENT(r.review_text) AS sentiment_score,
    -- Add standardized text with translations
    CASE
        WHEN CONTAINS(LOWER(r.review_language), 'german') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'de', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'french') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'fr', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'spanish') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'es', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'italian') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'it', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'portuguese') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'pt', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'chinese') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'zh', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'japanese') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ja', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'korean') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ko', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'russian') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ru', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'arabic') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ar', 'en')
        ELSE r.review_text
    END AS review_text_english
FROM {{ ref('stg_product_reviews') }} r
WHERE r.review_text IS NOT NULL 