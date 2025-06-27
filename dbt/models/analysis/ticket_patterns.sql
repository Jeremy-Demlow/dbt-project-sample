-- Ticket patterns model that analyzes support ticket patterns
-- This model aggregates ticket information to identify patterns and trends
-- Uses array aggregation to track ticket categories and priorities over time

SELECT
    customer_id,
    COUNT(*) AS ticket_count,
    MIN(ticket_date) AS first_ticket_date,
    MAX(ticket_date) AS most_recent_ticket_date,
    ARRAY_AGG(ticket_category) WITHIN GROUP (ORDER BY ticket_date) AS ticket_categories,
    ARRAY_AGG(priority_level) WITHIN GROUP (ORDER BY ticket_date) AS ticket_priorities
FROM {{ ref('fact_support_tickets') }}
GROUP BY customer_id 