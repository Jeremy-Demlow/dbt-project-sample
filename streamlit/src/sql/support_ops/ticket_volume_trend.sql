SELECT
    DATE(ticket_date) as date,
    COUNT(*) as ticket_count
FROM ANALYTICS.FACT_SUPPORT_TICKETS
WHERE ticket_date BETWEEN :start_date AND :end_date
GROUP BY 1
ORDER BY 1; 