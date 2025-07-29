{{
  config(
    materialized='semantic_view'
  )
}}

-- Customer Intelligence Semantic View Documentation
-- 
-- This file serves as documentation for the semantic view.
-- The actual semantic view is created using a macro.
--
-- To create/update the semantic view, run:
-- dbt run-operation create_customer_intelligence_semantic_view
--
-- The semantic view provides a comprehensive business model for customer analytics,
-- combining customer interactions, reviews, support tickets, and personas 
-- to enable natural language queries through Cortex Analyst.

CREATE OR REPLACE SEMANTIC VIEW {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.customer_intelligence_semantic_view

  TABLES (
    -- Core customer dimension table
    customers AS {{ var('dbt_cortex_database') }}.{{ var('analytics_schema') }}.CUSTOMER_PERSONA_SIGNALS
      PRIMARY KEY (customer_id)
      WITH SYNONYMS ('customer personas', 'customer profiles', 'customer signals')
      COMMENT = 'Comprehensive customer profile with personas, sentiment, and behavior signals',
    
    -- Customer interactions fact table
    interactions AS {{ var('dbt_cortex_database') }}.{{ var('analytics_schema') }}.FACT_CUSTOMER_INTERACTIONS
      PRIMARY KEY (interaction_id)
      WITH SYNONYMS ('customer interactions', 'service interactions', 'agent interactions')
      COMMENT = 'Customer service interactions with sentiment analysis',
    
    -- Product reviews fact table
    reviews AS {{ var('dbt_cortex_database') }}.{{ var('analytics_schema') }}.FACT_PRODUCT_REVIEWS
      PRIMARY KEY (review_id)
      WITH SYNONYMS ('product reviews', 'customer reviews', 'product feedback')
      COMMENT = 'Product reviews with ratings, sentiment, and translations',
    
    -- Support tickets fact table
    tickets AS {{ var('dbt_cortex_database') }}.{{ var('analytics_schema') }}.FACT_SUPPORT_TICKETS
      PRIMARY KEY (ticket_id)
      WITH SYNONYMS ('support tickets', 'customer tickets', 'service tickets')
      COMMENT = 'Support tickets with priority classification and sentiment analysis'
  )

  RELATIONSHIPS (
    -- Link interactions to customers
    interactions_to_customers AS
      interactions (customer_id) REFERENCES customers,
    
    -- Link reviews to customers
    reviews_to_customers AS
      reviews (customer_id) REFERENCES customers,
    
    -- Link tickets to customers  
    tickets_to_customers AS
      tickets (customer_id) REFERENCES customers
  )

  FACTS (
    -- Customer-level facts
    customers.customer_lifetime_value AS lifetime_value
      COMMENT = 'Total lifetime value of the customer',
    customers.customer_products_owned AS products_owned
      COMMENT = 'Number of products owned by the customer',
    customers.customer_avg_sentiment AS avg_sentiment
      COMMENT = 'Average sentiment score across all customer touchpoints',
    customers.customer_sentiment_volatility AS sentiment_volatility
      COMMENT = 'Volatility in customer sentiment scores',
    customers.customer_ticket_count AS ticket_count
      COMMENT = 'Total number of support tickets for the customer',
    customers.customer_avg_rating AS avg_rating
      COMMENT = 'Average product review rating given by the customer',
    
    -- Interaction-level facts
    interactions.interaction_sentiment AS sentiment_score
      COMMENT = 'Sentiment score of the customer interaction',
    
    -- Review-level facts
    reviews.review_rating AS review_rating
      COMMENT = 'Product review rating (1-5 scale)',
    reviews.review_sentiment AS sentiment_score
      COMMENT = 'Sentiment score of the product review',
    
    -- Ticket-level facts
    tickets.ticket_sentiment AS sentiment_score
      COMMENT = 'Sentiment score of the support ticket'
  )

  DIMENSIONS (
    -- Customer dimensions
    customers.customer_id AS customer_id
      WITH SYNONYMS ('customer identifier', 'customer ID')
      COMMENT = 'Unique identifier for the customer',
    customers.customer_persona AS persona
      WITH SYNONYMS ('customer type', 'customer segment')
      COMMENT = 'Original customer persona classification',
    customers.customer_derived_persona AS derived_persona
      WITH SYNONYMS ('calculated persona', 'behavior-based persona')
      COMMENT = 'AI-derived customer persona based on behavior patterns',
    customers.customer_churn_risk AS churn_risk
      WITH SYNONYMS ('churn likelihood', 'retention risk')
      COMMENT = 'Predicted customer churn risk level',
    customers.customer_upsell_opportunity AS upsell_opportunity
      WITH SYNONYMS ('upsell potential', 'growth opportunity')
      COMMENT = 'Predicted customer upsell opportunity level',
    customers.customer_overall_sentiment AS overall_sentiment
      WITH SYNONYMS ('sentiment category', 'customer sentiment')
      COMMENT = 'Categorized overall customer sentiment',
    customers.customer_sign_up_date AS sign_up_date
      WITH SYNONYMS ('registration date', 'join date')
      COMMENT = 'Date when the customer first registered',
    customers.customer_sign_up_year AS YEAR(sign_up_date)
      WITH SYNONYMS ('sign up year', 'registration year')
      COMMENT = 'Year when the customer signed up',
    customers.customer_sign_up_month AS DATE_TRUNC('month', sign_up_date)
      WITH SYNONYMS ('sign up month', 'registration month')
      COMMENT = 'Month when the customer signed up',
    
    -- Interaction dimensions
    interactions.interaction_date AS interaction_date
      WITH SYNONYMS ('contact date', 'service date')
      COMMENT = 'Date when the customer interaction occurred',
    interactions.interaction_type AS interaction_type
      WITH SYNONYMS ('contact type', 'communication channel')
      COMMENT = 'Type of customer interaction (call, email, chat)',
    interactions.agent_id AS agent_id
      WITH SYNONYMS ('service agent', 'representative ID')
      COMMENT = 'Customer service agent who handled the interaction',
    interactions.interaction_month AS DATE_TRUNC('month', interaction_date)
      WITH SYNONYMS ('interaction month', 'contact month')
      COMMENT = 'Month of the customer interaction',
    interactions.interaction_year AS YEAR(interaction_date)
      WITH SYNONYMS ('interaction year', 'contact year')
      COMMENT = 'Year of the customer interaction',
    
    -- Review dimensions
    reviews.review_date AS review_date
      WITH SYNONYMS ('feedback date', 'rating date')
      COMMENT = 'Date when the product review was submitted',
    reviews.product_id AS product_id
      WITH SYNONYMS ('product identifier', 'item ID')
      COMMENT = 'Unique identifier for the reviewed product',
    reviews.review_language AS review_language
      WITH SYNONYMS ('feedback language', 'review language')
      COMMENT = 'Language of the original review text',
    reviews.review_month AS DATE_TRUNC('month', review_date)
      WITH SYNONYMS ('review month', 'feedback month')
      COMMENT = 'Month of the product review',
    reviews.review_year AS YEAR(review_date)
      WITH SYNONYMS ('review year', 'feedback year')
      COMMENT = 'Year of the product review',
    
    -- Ticket dimensions
    tickets.ticket_date AS ticket_date
      WITH SYNONYMS ('support date', 'issue date')
      COMMENT = 'Date when the support ticket was created',
    tickets.ticket_status AS ticket_status
      WITH SYNONYMS ('ticket state', 'issue status')
      COMMENT = 'Current status of the support ticket',
    tickets.ticket_category AS ticket_category
      WITH SYNONYMS ('issue category', 'problem type')
      COMMENT = 'Category classification of the support issue',
    tickets.priority_level AS priority_level
      WITH SYNONYMS ('ticket priority', 'urgency level')
      COMMENT = 'AI-classified priority level of the support ticket',
    tickets.ticket_month AS DATE_TRUNC('month', ticket_date)
      WITH SYNONYMS ('ticket month', 'support month')
      COMMENT = 'Month of the support ticket',
    tickets.ticket_year AS YEAR(ticket_date)
      WITH SYNONYMS ('ticket year', 'support year')
      COMMENT = 'Year of the support ticket'
  )

  METRICS (
    -- Customer metrics
    customers.total_customers AS COUNT(customer_id)
      COMMENT = 'Total number of unique customers',
    customers.average_lifetime_value AS AVG(customers.customer_lifetime_value)
      COMMENT = 'Average customer lifetime value',
    customers.total_lifetime_value AS SUM(customers.customer_lifetime_value)
      COMMENT = 'Total lifetime value across all customers',
    customers.average_products_owned AS AVG(customers.customer_products_owned)
      COMMENT = 'Average number of products owned per customer',
    customers.average_customer_sentiment AS AVG(customers.customer_avg_sentiment)
      COMMENT = 'Average sentiment score across all customers',
    customers.high_churn_risk_customers AS COUNT(CASE WHEN customers.customer_churn_risk = 'High' THEN 1 END)
      COMMENT = 'Number of customers with high churn risk',
    customers.high_upsell_opportunity_customers AS COUNT(CASE WHEN customers.customer_upsell_opportunity = 'High' THEN 1 END)
      COMMENT = 'Number of customers with high upsell opportunity',
    
    -- Interaction metrics
    interactions.total_interactions AS COUNT(interaction_id)
      COMMENT = 'Total number of customer interactions',
    interactions.average_interaction_sentiment AS AVG(interactions.interaction_sentiment)
      COMMENT = 'Average sentiment score of customer interactions',
    interactions.total_interaction_customers AS COUNT(DISTINCT interactions.customer_id)
      COMMENT = 'Number of unique customers with interactions',
    
    -- Review metrics
    reviews.total_reviews AS COUNT(review_id)
      COMMENT = 'Total number of product reviews',
    reviews.average_review_rating AS AVG(reviews.review_rating)
      COMMENT = 'Average product review rating',
    reviews.average_review_sentiment AS AVG(reviews.review_sentiment)
      COMMENT = 'Average sentiment score of product reviews',
    reviews.total_review_customers AS COUNT(DISTINCT reviews.customer_id)
      COMMENT = 'Number of unique customers with reviews',
    
    -- Ticket metrics
    tickets.total_tickets AS COUNT(ticket_id)
      COMMENT = 'Total number of support tickets',
    tickets.average_ticket_sentiment AS AVG(tickets.ticket_sentiment)
      COMMENT = 'Average sentiment score of support tickets',
    tickets.total_ticket_customers AS COUNT(DISTINCT tickets.customer_id)
      COMMENT = 'Number of unique customers with tickets',
    tickets.critical_tickets AS COUNT(CASE WHEN tickets.priority_level = 'Critical' THEN 1 END)
      COMMENT = 'Number of critical priority support tickets',
    tickets.high_priority_tickets AS COUNT(CASE WHEN tickets.priority_level = 'High' THEN 1 END)
      COMMENT = 'Number of high priority support tickets'
  )

  COMMENT = 'Comprehensive semantic view for customer intelligence analytics, enabling natural language queries about customer behavior, sentiment, support issues, and business opportunities' 