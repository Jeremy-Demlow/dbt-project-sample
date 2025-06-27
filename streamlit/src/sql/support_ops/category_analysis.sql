SELECT
    DATE(ticket_date) as date,
    ticket_category as category,
    COUNT(*) as ticket_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(ticket_date)) as FLOAT) as percentage
FROM ANALYTICS.FACT_SUPPORT_TICKETS
WHERE ticket_date BETWEEN :start_date AND :end_date
GROUP BY 1, 2
ORDER BY 1, 3 DESC; 