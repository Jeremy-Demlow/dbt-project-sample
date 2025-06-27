-- Customer persona signals model that analyzes customer behavior patterns
-- This model combines various signals to classify customer personas and predict behavior
-- Uses sentiment analysis, support history, and other metrics to create customer profiles

SELECT 
    cb.customer_id,
    
    -- Base customer information
    cb.persona,
    cb.sign_up_date,
    cb.products_owned,
    cb.lifetime_value,
    
    -- Sentiment analysis
    st.avg_sentiment,
    st.sentiment_trend,
    st.sentiment_volatility,
    CASE 
        WHEN st.avg_sentiment < -0.3 THEN 'Negative'
        WHEN st.avg_sentiment > 0.3 THEN 'Positive'
        ELSE 'Neutral'
    END AS overall_sentiment,
    
    -- Support history
    tpat.ticket_count,
    tpat.ticket_categories,
    tpat.ticket_priorities,
    
    -- Review ratings
    AVG(pr.review_rating) AS avg_rating,
    
    -- Customer summary
    is_summary.customer_summary,
    
    -- Customer persona
    CASE
        WHEN st.avg_sentiment > 0.5 AND COALESCE(tpat.ticket_count, 0) <= 1 THEN 'Satisfied'
        WHEN st.avg_sentiment < -0.1 AND COALESCE(tpat.ticket_count, 0) >= 1 THEN 'Frustrated'
        WHEN st.sentiment_volatility > 0.95 THEN 'Mixed'
        WHEN st.sentiment_trend > 0.3 THEN 'Improving'
        WHEN st.sentiment_trend < -0.3 THEN 'Deteriorating'
        ELSE 'Neutral'
    END AS derived_persona,
    
    -- Churn risk
    CASE
        WHEN st.avg_sentiment < -0.1 AND COALESCE(tpat.ticket_count, 0) >= 1 THEN 'High'
        WHEN st.avg_sentiment < -0.05 THEN 'High'
        WHEN st.avg_sentiment < 0 OR (COALESCE(tpat.ticket_count, 0) >= 1 AND st.sentiment_trend < 0) THEN 'Medium'
        WHEN st.sentiment_trend < 0 THEN 'Medium'
        ELSE 'Low'
    END AS churn_risk,
    
    -- Upsell opportunity
    CASE
        WHEN st.avg_sentiment > 0.3 AND COALESCE(tpat.ticket_count, 0) <= 1 THEN 'High'
        WHEN st.sentiment_trend > 0.3 THEN 'Medium'
        ELSE 'Low'
    END AS upsell_opportunity
FROM {{ ref('stg_customers') }} cb
LEFT JOIN {{ ref('sentiment_trends') }} st USING (customer_id)
LEFT JOIN {{ ref('insight_summaries') }} is_summary USING (customer_id)
LEFT JOIN {{ ref('ticket_patterns') }} tpat USING (customer_id)
LEFT JOIN {{ ref('fact_product_reviews') }} pr USING (customer_id)
GROUP BY 
    cb.customer_id,
    cb.persona,
    cb.sign_up_date,
    cb.products_owned,
    cb.lifetime_value,
    st.avg_sentiment, 
    st.sentiment_trend, 
    st.sentiment_volatility,
    tpat.ticket_count,
    tpat.ticket_categories,
    tpat.ticket_priorities,
    is_summary.customer_summary 