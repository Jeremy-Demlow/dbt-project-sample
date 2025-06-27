-- Rating Distribution Analysis
SELECT
    review_rating,
    COUNT(*) as count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as FLOAT) as percentage
FROM ANALYTICS.FACT_PRODUCT_REVIEWS
GROUP BY 1
ORDER BY 1; 