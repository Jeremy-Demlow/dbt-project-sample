WITH resolution_metrics AS (
    SELECT
        DATE(ticket_date) as date,
        COUNT(*) as total_tickets,
        SUM(CASE 
            WHEN LOWER(ticket_status) IN ('resolved', 'closed') THEN 1 
            ELSE 0 
        END) as resolved_tickets,
        AVG(CASE 
            WHEN LOWER(ticket_status) IN ('resolved', 'closed') THEN 
                DATEDIFF('hour', ticket_date, 
                    -- Using expected_resolution_timeframe as proxy for resolution date
                    -- In a real system, you'd have a resolution_date column
                    DATEADD('hour', 
                        CASE priority_level
                            WHEN 'Critical' THEN 4   -- 4 hours for Critical
                            WHEN 'High' THEN 24      -- 24 hours for High
                            WHEN 'Medium' THEN 72    -- 72 hours for Medium
                            ELSE 168                 -- 1 week for Low
                        END,
                        ticket_date
                    )
                )
            ELSE NULL 
        END) as avg_resolution_time_hours
    FROM ANALYTICS.FACT_SUPPORT_TICKETS
    WHERE ticket_date BETWEEN :start_date AND :end_date
    GROUP BY 1
)
SELECT
    date,
    total_tickets,
    resolved_tickets,
    (resolved_tickets * 100.0 / NULLIF(total_tickets, 0)) as resolution_rate,
    avg_resolution_time_hours
FROM resolution_metrics
ORDER BY date; 