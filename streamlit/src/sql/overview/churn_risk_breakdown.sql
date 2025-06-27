-- Churn risk breakdown by persona
SELECT
    cps.persona as PERSONA,
    cps.churn_risk as CHURN_RISK,
    COUNT(DISTINCT cps.customer_id) as CUSTOMER_COUNT,
    CAST((COUNT(DISTINCT cps.customer_id) * 100.0 / SUM(COUNT(DISTINCT cps.customer_id)) OVER (PARTITION BY cps.persona)) as FLOAT) as PERCENTAGE
FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps
WHERE cps.persona IS NOT NULL
GROUP BY cps.persona, cps.churn_risk
ORDER BY 
    cps.persona,
    CASE cps.churn_risk
        WHEN 'High' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low' THEN 3
    END 