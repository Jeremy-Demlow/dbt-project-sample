WITH effort_metrics AS (
    SELECT
        DATE(ticket_date) as date,
        -- Calculate effort score (0-100 scale) based on multiple factors
        AVG(
            -- Priority level impact (40% weight)
            CASE priority_level
                WHEN 'Critical' THEN 100
                WHEN 'High' THEN 75
                WHEN 'Medium' THEN 50
                WHEN 'Low' THEN 25
            END * 0.4 +
            -- Sentiment impact (30% weight)
            -- Convert -1 to 1 scale to 0-100
            ((sentiment_score + 1) * 50) * 0.3 +
            -- Resolution time impact (30% weight)
            CASE 
                WHEN ticket_status = 'Resolved' THEN 
                    LEAST(100, 
                        100 - DATEDIFF('hour', ticket_date, 
                            DATEADD('hour', 
                                CASE priority_level
                                    WHEN 'Critical' THEN 4
                                    WHEN 'High' THEN 24
                                    WHEN 'Medium' THEN 72
                                    ELSE 168
                                END,
                                ticket_date
                            )
                        )
                    )
                ELSE 50  -- Neutral score for unresolved tickets
            END * 0.3
        ) as customer_effort_score,
        COUNT(*) as ticket_count
    FROM ANALYTICS.FACT_SUPPORT_TICKETS
    WHERE ticket_date BETWEEN :start_date AND :end_date
    GROUP BY 1
)
SELECT
    date,
    ROUND(customer_effort_score, 2) as customer_effort_score,
    ticket_count,
    -- Categorize effort scores
    CASE 
        WHEN customer_effort_score >= 80 THEN 'Low Effort'
        WHEN customer_effort_score >= 60 THEN 'Medium Effort'
        ELSE 'High Effort'
    END as effort_category
FROM effort_metrics
ORDER BY date; 