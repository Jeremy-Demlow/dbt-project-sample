WITH channel_metrics AS (
    SELECT
        DATE(i.interaction_date) as date,
        i.interaction_type as channel,
        COUNT(DISTINCT i.interaction_id) as total_interactions,
        AVG(i.sentiment_score) as avg_sentiment,
        COUNT(DISTINCT t.ticket_id) as related_tickets,
        SUM(CASE WHEN t.ticket_status = 'Resolved' THEN 1 ELSE 0 END) as resolved_tickets,
        AVG(CASE 
            WHEN t.ticket_status = 'Resolved' THEN 
                DATEDIFF('hour', t.ticket_date, 
                    DATEADD('hour', 
                        CASE t.priority_level
                            WHEN 'Critical' THEN 4
                            WHEN 'High' THEN 24
                            WHEN 'Medium' THEN 72
                            ELSE 168
                        END,
                        t.ticket_date
                    )
                )
            ELSE NULL 
        END) as avg_resolution_time_hours
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS i
    LEFT JOIN ANALYTICS.FACT_SUPPORT_TICKETS t 
        ON i.customer_id = t.customer_id
        AND t.ticket_date >= i.interaction_date
        AND t.ticket_date <= DATEADD('hour', 24, i.interaction_date)
    WHERE i.interaction_date BETWEEN :start_date AND :end_date
    GROUP BY 1, 2
)
SELECT
    date,
    channel,
    total_interactions,
    avg_sentiment,
    related_tickets,
    resolved_tickets,
    -- Calculate channel effectiveness score (0-100)
    ROUND(
        (
            -- Resolution rate (40% weight) - handle division by zero
            CASE 
                WHEN related_tickets > 0 THEN 
                    (COALESCE(resolved_tickets, 0) * 100.0 / related_tickets) * 0.4 
                ELSE 0 
            END +
            -- Sentiment score (40% weight)
            ((avg_sentiment + 1) * 50) * 0.4 +
            -- Resolution time score (20% weight)
            (100 - LEAST(100, COALESCE(avg_resolution_time_hours, 0))) * 0.2
        ),
        2
    ) as channel_effectiveness_score
FROM channel_metrics
ORDER BY date, channel_effectiveness_score DESC; 