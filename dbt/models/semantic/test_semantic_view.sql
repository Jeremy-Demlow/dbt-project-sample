{{
  config(
    materialized='ephemeral'
  )
}}

-- =============================================================================
-- Semantic View Test & Validation Script
-- =============================================================================
-- This script demonstrates proper usage of the customer_intelligence_semantic_view
-- and serves as validation that the semantic view is working correctly.
-- 
-- IMPORTANT: Due to granularity rules, each test must be run separately.
-- Semantic views restrict which dimensions and metrics can be combined.
-- =============================================================================

-- Test 1: Basic customer count (simplest validation)
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    METRICS total_customers
)

/*
=============================================================================
EXAMPLE CORTEX ANALYST QUESTIONS
=============================================================================

The following natural language questions can be asked in Snowsight Cortex Analyst:

📊 CUSTOMER ANALYTICS QUESTIONS:
- "How many customers do we have by persona?"
- "What is the average lifetime value for satisfied customers?"
- "Show me customers with high churn risk and negative sentiment"
- "Which customer personas have the highest upsell opportunities?"
- "What's the overall sentiment distribution across all customers?"
- "How many customers have high churn risk?"
- "Show me the average customer sentiment by derived persona"
- "What percentage of customers are in each churn risk category?"

🗣️ INTERACTION ANALYTICS QUESTIONS:
- "Which interaction types have the highest volume?"
- "What's the average sentiment for each interaction channel?"
- "Show me interaction sentiment trends by type"
- "Which communication channels perform best for customer satisfaction?"
- "How many customer interactions happened this year?"
- "Show me interaction trends by month"
- "What's the sentiment trend for customer interactions over time?"

⭐ PRODUCT REVIEW QUESTIONS:
- "What's the average product review rating?"
- "How many reviews do we have by language?"
- "Show me review sentiment by language"
- "What's the trend in review ratings over time?"

🎫 SUPPORT OPERATIONS QUESTIONS:
- "How many support tickets do we have by priority level?"
- "What's the distribution of critical vs high priority tickets?"
- "Show me ticket volume trends by category"
- "What's the average sentiment for support tickets?"
- "How many critical tickets were created this month?"
- "What's the ratio of high priority to total tickets?"
- "Show me ticket resolution patterns by status"

=============================================================================
ADDITIONAL SQL TEST EXAMPLES (Run each separately)
=============================================================================

-- Customer Analysis Tests:
-- ✅ Customer segmentation by persona and churn risk
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS customer_persona, customer_churn_risk
    METRICS total_customers, average_lifetime_value
) ORDER BY total_customers DESC;

-- ✅ Customer sentiment analysis
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS customer_overall_sentiment, customer_churn_risk
    METRICS total_customers, average_customer_sentiment
) ORDER BY total_customers DESC;

-- Interaction Analysis Tests:
-- ✅ Channel effectiveness analysis
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS interaction_type
    METRICS total_interactions, average_interaction_sentiment
) ORDER BY total_interactions DESC;

-- ✅ Temporal interaction patterns
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS interaction_year, interaction_month
    METRICS total_interactions, average_interaction_sentiment
) ORDER BY interaction_year, interaction_month;

-- Review Analysis Tests:
-- ✅ Review language analysis
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS review_language
    METRICS total_reviews, average_review_rating, average_review_sentiment
) ORDER BY total_reviews DESC;

-- ✅ Review temporal patterns
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS review_year, review_month
    METRICS total_reviews, average_review_rating
) ORDER BY review_year, review_month;

-- Support Operations Tests:
-- ✅ Priority analysis
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS priority_level
    METRICS total_tickets, critical_tickets, high_priority_tickets
) ORDER BY total_tickets DESC;

-- ✅ Category and status breakdown
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS ticket_category, ticket_status
    METRICS total_tickets, average_ticket_sentiment
) ORDER BY total_tickets DESC;

=============================================================================
GRANULARITY RULE EXAMPLES
=============================================================================

-- ❌ INVALID: Cannot mix dimensions from different granularity levels
-- This would fail with: "Invalid dimension specified: The dimension entity 
-- 'TICKETS' must be related to and have an equal or lower level of granularity"

SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS interaction_type, priority_level  -- Different tables!
    METRICS total_interactions, total_tickets     -- Different granularities!
);

-- ✅ VALID: Same granularity level (customer-level)
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS customer_persona, customer_churn_risk
    METRICS total_customers, average_lifetime_value
);

-- ✅ VALID: Same granularity level (interaction-level) 
SELECT * FROM SEMANTIC_VIEW(
    {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view
    DIMENSIONS interaction_type
    METRICS total_interactions, average_interaction_sentiment
);

=============================================================================
EXPECTED RESULTS FOR VALIDATION
=============================================================================

Based on our sample data (1000 customers), you should expect:
- total_customers: 1000
- Customer personas: Mix of "Satisfied", "Frustrated", "Neutral", etc.
- Churn risk levels: "High", "Medium", "Low" 
- Interaction types: "Email", "Phone", "Chat", "Social Media"
- Priority levels: "Critical", "High", "Medium", "Low"
- Languages: "en", "es", "fr", "de"

If any queries return 0 results, check that the underlying fact tables
have been populated with data.

*/ 