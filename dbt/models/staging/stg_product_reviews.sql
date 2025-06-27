-- Staging model for product reviews
-- This model extracts and standardizes product review information from the raw JSON data
-- Fields are cast to appropriate data types and renamed for clarity

SELECT
    data:review_id::VARCHAR AS review_id,
    data:customer_id::VARCHAR AS customer_id,
    data:product_id::VARCHAR AS product_id,
    TRY_TO_TIMESTAMP_NTZ(data:review_date::VARCHAR) AS review_date,
    data:review_rating::NUMBER AS review_rating,
    data:review_text::VARCHAR AS review_text,
    data:review_language::VARCHAR AS review_language
FROM {{ source('raw', 'product_reviews') }} 