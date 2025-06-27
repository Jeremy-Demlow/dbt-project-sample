WITH ticket_response_times AS (
    SELECT
        DATE(ticket_date) as date,
        AVG(
            CASE 
                WHEN ticket_status = 'New' THEN NULL  -- Exclude unresponded tickets
                ELSE DATEDIFF('minute', ticket_date, 
                    -- Using expected_resolution_timeframe as proxy for first response
                    -- In a real system, you'd have a first_response_date column
                    DATEADD('minute', 
                        CASE priority_level
                            WHEN 'Critical' THEN 30   -- 30 min response time for Critical
                            WHEN 'High' THEN 120      -- 2 hours for High
                            WHEN 'Medium' THEN 480    -- 8 hours for Medium
                            ELSE 1440                 -- 24 hours for Low
                        END,
                        ticket_date
                    )
                )
            END
        ) as avg_response_time_minutes,
        COUNT(*) as ticket_count,
        SUM(CASE WHEN ticket_status != 'New' THEN 1 ELSE 0 END) as responded_tickets
    FROM ANALYTICS.FACT_SUPPORT_TICKETS
    WHERE ticket_date BETWEEN :start_date AND :end_date
    GROUP BY 1
)
SELECT
    date,
    avg_response_time_minutes,
    ticket_count,
    responded_tickets,
    (responded_tickets * 100.0 / NULLIF(ticket_count, 0)) as response_rate
FROM ticket_response_times
ORDER BY date; 