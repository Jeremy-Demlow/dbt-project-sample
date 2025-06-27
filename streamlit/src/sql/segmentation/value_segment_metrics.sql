-- sql/segmentation/value_segment_metrics.sql
-- Provides data for Value Segment Radar chart.
-- KPI related columns (SEGMENT_HEALTH_SCORE, HIGH_VALUE_CUSTOMER_PERCENT, etc.) have been removed.

WITH radar_chart_metrics AS (
    SELECT
        DERIVED_PERSONA AS SEGMENT,
        AVG(AVG_SENTIMENT) AS AVG_SENTIMENT_SCORE,
        AVG(SENTIMENT_VOLATILITY) AS AVG_SENTIMENT_VOLATILITY,
        AVG(AVG_RATING) AS AVG_CUSTOMER_RATING,
        AVG(TICKET_COUNT) AS AVG_TICKET_COUNT
    FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS
    WHERE DERIVED_PERSONA IS NOT NULL AND DERIVED_PERSONA != '' -- Ensure segment is not null or empty
    GROUP BY 1
)
-- Final SELECT statement for radar chart data.
SELECT 
    rcm.SEGMENT, 
    'Avg Sentiment Score' AS METRIC_NAME, 
    ROUND(rcm.AVG_SENTIMENT_SCORE, 2) AS METRIC_VALUE
FROM radar_chart_metrics rcm
UNION ALL
SELECT 
    rcm.SEGMENT, 
    'Avg Sentiment Volatility' AS METRIC_NAME, 
    ROUND(rcm.AVG_SENTIMENT_VOLATILITY, 2) AS METRIC_VALUE
FROM radar_chart_metrics rcm
UNION ALL
SELECT 
    rcm.SEGMENT, 
    'Avg Customer Rating' AS METRIC_NAME, 
    ROUND(rcm.AVG_CUSTOMER_RATING, 2) AS METRIC_VALUE
FROM radar_chart_metrics rcm
UNION ALL
SELECT 
    rcm.SEGMENT, 
    'Avg Ticket Count' AS METRIC_NAME, 
    ROUND(rcm.AVG_TICKET_COUNT, 1) AS METRIC_VALUE
FROM radar_chart_metrics rcm
ORDER BY SEGMENT, METRIC_NAME; 