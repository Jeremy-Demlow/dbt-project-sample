-- Create database and schemas
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS CORTEX_WH 
WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
USE WAREHOUSE CORTEX_WH;

CREATE ROLE IF NOT EXISTS DBT_ROLE;

-- Grant DBT_ROLE comprehensive privileges for dbt operations
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DBT_ROLE;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DBT_ROLE;

-- Warehouse usage and management
GRANT ALL PRIVILEGES ON WAREHOUSE CORTEX_WH TO ROLE DBT_ROLE;

-- Grant roles to users
GRANT ROLE DBT_ROLE TO USER JDEMLOW;
GRANT ROLE DBT_ROLE TO USER jd_service_account_admin;

-- Dynamic grant for DBT_ROLE to current user
DECLARE
    sql_command STRING;
BEGIN
    sql_command := 'GRANT ROLE DBT_ROLE TO USER "' || CURRENT_USER() || '";';
    EXECUTE IMMEDIATE sql_command;
    RETURN 'Role DBT_ROLE granted successfully to user ' || CURRENT_USER();
END;

USE ROLE DBT_ROLE;

DROP DATABASE IF EXISTS DBT_CORTEX_LLMS;

USE ROLE DBT_ROLE;
CREATE DATABASE IF NOT EXISTS DBT_CORTEX_LLMS;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.RAW;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.SEMANTIC_MODELS;

DROP DATABASE IF EXISTS DBT_CORTEX_LLMS;

USE ROLE DBT_ROLE;
CREATE DATABASE IF NOT EXISTS DBT_CORTEX_LLMS;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.RAW;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.SEMANTIC_MODELS;

-- Grant comprehensive database and schema permissions to DBT_ROLE
GRANT ALL PRIVILEGES ON DATABASE DBT_CORTEX_LLMS TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA DBT_CORTEX_LLMS.ANALYTICS TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA DBT_CORTEX_LLMS.SEMANTIC_MODELS TO ROLE DBT_ROLE;

-- Grant table-level permissions for future tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DBT_CORTEX_LLMS.ANALYTICS TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DBT_CORTEX_LLMS.SEMANTIC_MODELS TO ROLE DBT_ROLE;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA DBT_CORTEX_LLMS.ANALYTICS TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA DBT_CORTEX_LLMS.SEMANTIC_MODELS TO ROLE DBT_ROLE;

-- Grant permissions for stages and file formats
GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON ALL FILE FORMATS IN SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA DBT_CORTEX_LLMS.RAW TO ROLE DBT_ROLE;

--This SQL Script mimics the dbt project for loading and modelling the data in Snowflake
-- Set the role for this session
USE DATABASE DBT_CORTEX_LLMS;

-- 📄 File Format Setup - Making JSON feel at home
CREATE OR REPLACE FILE FORMAT RAW.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO';

CREATE OR REPLACE STAGE RAW.RAW_DATA_STAGE;

-- Upload customer interactions
PUT file://data/samples/1000_Customers/customer_interactions.json @RAW.RAW_DATA_STAGE;

-- Upload product reviews
PUT file://data/samples/1000_Customers/product_reviews.json @RAW.RAW_DATA_STAGE;

-- Upload support tickets
PUT file://data/samples/1000_Customers/support_tickets.json @RAW.RAW_DATA_STAGE;

-- Upload customers
PUT file://data/samples/1000_Customers/customers.json @RAW.RAW_DATA_STAGE;

-- 🎯 Raw Data Tables - The foundation of our data castle
CREATE OR REPLACE TRANSIENT TABLE RAW.CUSTOMER_INTERACTIONS (data VARIANT);
CREATE OR REPLACE TRANSIENT TABLE RAW.PRODUCT_REVIEWS (data VARIANT);
CREATE OR REPLACE TRANSIENT TABLE RAW.SUPPORT_TICKETS (data VARIANT);
CREATE OR REPLACE TRANSIENT TABLE RAW.CUSTOMERS (data VARIANT);

-- 📥 Data Loading - Let's fill these tables with good vibes
COPY INTO RAW.CUSTOMER_INTERACTIONS
FROM @RAW.RAW_DATA_STAGE/customer_interactions.json
FILE_FORMAT = RAW.JSON_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW.PRODUCT_REVIEWS
FROM @RAW.RAW_DATA_STAGE/product_reviews.json
FILE_FORMAT = RAW.JSON_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW.SUPPORT_TICKETS
FROM @RAW.RAW_DATA_STAGE/support_tickets.json
FILE_FORMAT = RAW.JSON_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW.CUSTOMERS
FROM @RAW.RAW_DATA_STAGE/customers.json
FILE_FORMAT = RAW.JSON_FORMAT
ON_ERROR = 'CONTINUE';

-- 🧹 Staging Layer - Clean and transform that data!
-- Customer Interactions
CREATE OR REPLACE TABLE ANALYTICS.STG_CUSTOMER_INTERACTIONS AS
SELECT
    data:interaction_id::VARCHAR AS interaction_id,
    data:customer_id::VARCHAR AS customer_id,
    TRY_TO_TIMESTAMP_NTZ(data:interaction_date::VARCHAR) AS interaction_date,
    data:agent_id::VARCHAR AS agent_id,
    data:interaction_type::VARCHAR AS interaction_type,
    data:interaction_notes::VARCHAR AS interaction_notes
FROM RAW.CUSTOMER_INTERACTIONS;

-- Product Reviews
CREATE OR REPLACE TABLE ANALYTICS.STG_PRODUCT_REVIEWS AS
SELECT
    data:review_id::VARCHAR AS review_id,
    data:customer_id::VARCHAR AS customer_id,
    data:product_id::VARCHAR AS product_id,
    TRY_TO_TIMESTAMP_NTZ(data:review_date::VARCHAR) AS review_date,
    data:review_rating::NUMBER AS review_rating,
    data:review_text::VARCHAR AS review_text,
    data:review_language::VARCHAR AS review_language
FROM RAW.PRODUCT_REVIEWS;

-- Support Tickets
CREATE OR REPLACE TABLE ANALYTICS.STG_SUPPORT_TICKETS AS
SELECT
    data:ticket_id::VARCHAR AS ticket_id,
    data:customer_id::VARCHAR AS customer_id,
    TRY_TO_TIMESTAMP_NTZ(data:ticket_date::VARCHAR) AS ticket_date,
    data:ticket_status::VARCHAR AS ticket_status,
    data:ticket_category::VARCHAR AS ticket_category,
    data:ticket_description::VARCHAR AS ticket_description
FROM RAW.SUPPORT_TICKETS;

-- Customer Dimension
CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_BASE AS
SELECT
    data:customer_id::VARCHAR AS customer_id,
    data:persona::VARCHAR AS persona,
    TRY_TO_DATE(data:sign_up_date::VARCHAR) AS sign_up_date,
    data:products_owned::NUMBER AS products_owned,
    data:lifetime_value::NUMBER AS lifetime_value
FROM RAW.CUSTOMERS;

-- 📊 Fact Tables - Where the real insights live
-- Customer Interactions Fact
CREATE OR REPLACE TABLE ANALYTICS.FACT_CUSTOMER_INTERACTIONS AS
SELECT
    i.interaction_id,
    i.customer_id,
    i.interaction_date,
    i.agent_id,
    i.interaction_type,
    i.interaction_notes,
    -- Add sentiment analysis
    SNOWFLAKE.CORTEX.SENTIMENT(i.interaction_notes) AS sentiment_score
FROM ANALYTICS.STG_CUSTOMER_INTERACTIONS i
WHERE i.interaction_notes IS NOT NULL;

-- Product Reviews Fact
CREATE OR REPLACE TABLE ANALYTICS.FACT_PRODUCT_REVIEWS AS
SELECT
    r.review_id,
    r.customer_id,
    r.product_id,
    r.review_date,
    r.review_rating,
    r.review_text,
    r.review_language,
    -- Add sentiment analysis
    SNOWFLAKE.CORTEX.SENTIMENT(r.review_text) AS sentiment_score,
    -- Add standardized text
    CASE
        WHEN CONTAINS(LOWER(r.review_language), 'german') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'de', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'french') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'fr', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'spanish') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'es', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'italian') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'it', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'portuguese') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'pt', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'chinese') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'zh', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'japanese') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ja', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'korean') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ko', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'russian') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ru', 'en')
        WHEN CONTAINS(LOWER(r.review_language), 'arabic') THEN SNOWFLAKE.CORTEX.TRANSLATE(r.review_text, 'ar', 'en')
        ELSE r.review_text
    END AS review_text_english
FROM ANALYTICS.STG_PRODUCT_REVIEWS r
WHERE r.review_text IS NOT NULL;

-- Support Tickets Fact
CREATE OR REPLACE TABLE ANALYTICS.FACT_SUPPORT_TICKETS AS
SELECT
    t.ticket_id,
    t.customer_id,
    t.ticket_date,
    t.ticket_status,
    t.ticket_category,
    t.ticket_description,
    -- Add sentiment analysis
    SNOWFLAKE.CORTEX.SENTIMENT(t.ticket_description) AS sentiment_score,
    -- Add priority classification
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        t.ticket_description,
        [
            {
                'label': 'Critical',
                'description': 'Requires immediate attention and resolution',
                'examples': [
                    'System is down and customers cannot place orders',
                    'Security breach detected',
                    'Payment processing completely stopped',
                    'All users locked out of the system',
                    'Cannot access my account',
                    'Order stuck in processing',
                    'Payment failed',
                    'Website not loading',
                    'Critical feature not working',
                    'Data loss or corruption'
                ]
            },
            {
                'label': 'High',
                'description': 'Should be resolved within 24 hours',
                'examples': [
                    'Customer cannot complete checkout',
                    'Order status stuck in processing',
                    'Unable to access account',
                    'Payment failed for multiple customers',
                    'Product not working as expected',
                    'Account access issues',
                    'Billing problems',
                    'Service disruption',
                    'Performance issues',
                    'Security concerns'
                ]
            },
            {
                'label': 'Medium',
                'description': 'Should be resolved within 3 days',
                'examples': [
                    'Product image not displaying correctly',
                    'Slow response times in certain areas',
                    'Minor UI issues',
                    'Account settings not saving',
                    'Feature not working as expected',
                    'Minor display issues',
                    'Non-critical performance problems',
                    'General functionality questions',
                    'Minor account issues',
                    'Non-urgent technical problems'
                ]
            },
            {
                'label': 'Low',
                'description': 'Can be handled in regular queue',
                'examples': [
                    'General product questions',
                    'Feature request',
                    'UI enhancement suggestion',
                    'Documentation clarification',
                    'How-to questions',
                    'Product information request',
                    'General feedback',
                    'Non-technical questions',
                    'Account information request',
                    'General support questions'
                ]
            }
        ],
        {
            'task_description': 'Classify the urgency level of this support ticket based on its description. When in doubt, classify as Critical or High priority if the issue affects core functionality or user access.'
        }
    )['label'] AS priority_level,
    -- Add customer expectations
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        'What specific timeframe or deadline does the customer mention or expect for resolution? ' || t.ticket_description
    ) AS expected_resolution_timeframe,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        'What compensation, refund, or specific remedy is the customer seeking? ' || t.ticket_description
    ) AS requested_remedy
FROM ANALYTICS.STG_SUPPORT_TICKETS t
WHERE t.ticket_description IS NOT NULL;

-- 🔍 Analysis Layer - Time to dig deep!
-- Sentiment Analysis
CREATE OR REPLACE TABLE ANALYTICS.SENTIMENT_ANALYSIS AS
WITH interaction_sentiment AS (
    SELECT
        customer_id,
        interaction_date,
        sentiment_score,
        'interaction' AS source_type
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
),
review_sentiment AS (
    SELECT
        customer_id,
        review_date AS interaction_date,
        sentiment_score,
        'review' AS source_type
    FROM ANALYTICS.FACT_PRODUCT_REVIEWS
),
ticket_sentiment AS (
    SELECT
        customer_id,
        ticket_date AS interaction_date,
        sentiment_score,
        'ticket' AS source_type
    FROM ANALYTICS.FACT_SUPPORT_TICKETS
)
SELECT * FROM interaction_sentiment
UNION ALL
SELECT * FROM review_sentiment
UNION ALL
SELECT * FROM ticket_sentiment;

-- Sentiment Trends Analysis
CREATE OR REPLACE TABLE ANALYTICS.SENTIMENT_TRENDS AS
WITH sentiment_data AS (
    SELECT
        customer_id,
        sentiment_score,
        interaction_date,
        FIRST_VALUE(sentiment_score) OVER (PARTITION BY customer_id ORDER BY interaction_date) AS first_sentiment,
        LAST_VALUE(sentiment_score) OVER (PARTITION BY customer_id ORDER BY interaction_date 
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sentiment
    FROM ANALYTICS.SENTIMENT_ANALYSIS
)
SELECT
    customer_id,
    ARRAY_AGG(sentiment_score) WITHIN GROUP (ORDER BY interaction_date) AS sentiment_history,
    AVG(sentiment_score) AS avg_sentiment,
    MIN(sentiment_score) AS min_sentiment,
    MAX(sentiment_score) AS max_sentiment,
    MAX(sentiment_score) - MIN(sentiment_score) AS sentiment_volatility,
    CASE
        WHEN COUNT(*) > 1 THEN 
            MAX(last_sentiment) - MAX(first_sentiment)
        ELSE 0
    END AS sentiment_trend
FROM sentiment_data
GROUP BY customer_id;

-- Ticket Pattern Analysis
CREATE OR REPLACE TABLE ANALYTICS.TICKET_PATTERNS AS
SELECT
    customer_id,
    COUNT(*) AS ticket_count,
    MIN(ticket_date) AS first_ticket_date,
    MAX(ticket_date) AS most_recent_ticket_date,
    ARRAY_AGG(ticket_category) WITHIN GROUP (ORDER BY ticket_date) AS ticket_categories,
    ARRAY_AGG(priority_level) WITHIN GROUP (ORDER BY ticket_date) AS ticket_priorities
FROM ANALYTICS.FACT_SUPPORT_TICKETS
GROUP BY customer_id;

-- Customer Insight Summaries
CREATE OR REPLACE TABLE ANALYTICS.INSIGHT_SUMMARIES AS
SELECT
    customer_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        [
            {
                'role': 'user',
                'content': 'Summarize the following customer interactions in 100 words or less: ' || 
                ARRAY_TO_STRING(
                    ARRAY_AGG(
                        CASE
                            WHEN interaction_notes IS NOT NULL THEN interaction_notes
                            WHEN review_text IS NOT NULL THEN review_text
                            WHEN ticket_description IS NOT NULL THEN ticket_description
                        END
                    ),
                    ' | '
                )
            }
        ],
        {
            'max_tokens': 100
        }
    ) AS customer_summary
FROM (
    SELECT customer_id, interaction_notes, NULL as review_text, NULL as ticket_description
    FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
    UNION ALL
    SELECT customer_id, NULL, review_text, NULL
    FROM ANALYTICS.FACT_PRODUCT_REVIEWS
    UNION ALL
    SELECT customer_id, NULL, NULL, ticket_description
    FROM ANALYTICS.FACT_SUPPORT_TICKETS
)
GROUP BY customer_id;

-- 👥 Customer Persona Analysis - Get to know your peeps
CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_PERSONA_SIGNALS AS
SELECT 
    cb.customer_id,
    
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
        WHEN st.avg_sentiment < -0.3 AND COALESCE(tpat.ticket_count, 0) >= 3 THEN 'Frustrated'
        WHEN st.sentiment_volatility > 0.7 THEN 'Mixed'
        WHEN st.sentiment_trend > 0.3 THEN 'Improving'
        WHEN st.sentiment_trend < -0.3 THEN 'Deteriorating'
        ELSE 'Neutral'
    END AS derived_persona,
    
    -- Churn risk
    CASE
        WHEN (st.avg_sentiment < -0.3 AND COALESCE(tpat.ticket_count, 0) >= 1) 
             OR (st.sentiment_trend < -0.2 AND COALESCE(tpat.ticket_count, 0) >= 1)
             OR (st.avg_sentiment < -0.2 AND st.sentiment_trend < -0.1) THEN 'High'
        WHEN (st.avg_sentiment < -0.1 AND st.sentiment_trend < 0) 
             OR (COALESCE(tpat.ticket_count, 0) >= 2) THEN 'Medium'
        ELSE 'Low'
    END AS churn_risk,
    
    -- Upsell opportunity
    CASE
        WHEN st.avg_sentiment > 0.3 AND COALESCE(tpat.ticket_count, 0) <= 1 THEN 'High'
        WHEN st.sentiment_trend > 0.3 THEN 'Medium'
        ELSE 'Low'
    END AS upsell_opportunity
FROM ANALYTICS.CUSTOMER_BASE cb
LEFT JOIN ANALYTICS.SENTIMENT_TRENDS st USING (customer_id)
LEFT JOIN ANALYTICS.INSIGHT_SUMMARIES is_summary USING (customer_id)
LEFT JOIN ANALYTICS.TICKET_PATTERNS tpat USING (customer_id)
LEFT JOIN ANALYTICS.FACT_PRODUCT_REVIEWS pr USING (customer_id)
GROUP BY 
    cb.customer_id, 
    st.avg_sentiment, 
    st.sentiment_trend, 
    st.sentiment_volatility,
    tpat.ticket_count,
    tpat.ticket_categories,
    tpat.ticket_priorities,
    is_summary.customer_summary;


-- 🎯 Time to Get Some Sweet Insights! 
-- 1. 📊 Customer Vibes Overview - How's everyone feeling?
SELECT 
    overall_sentiment,
    COUNT(*) as customer_count,
    ROUND(AVG(avg_sentiment), 2) as avg_sentiment_score,
    ROUND(AVG(avg_rating), 2) as avg_product_rating
FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS
GROUP BY overall_sentiment
ORDER BY customer_count DESC;

-- 2. 🚨 Churn Risk Check - Who's thinking about ghosting us?
SELECT 
    churn_risk,
    COUNT(*) as customer_count,
    ROUND(AVG(avg_sentiment), 2) as avg_sentiment_score,
    ROUND(AVG(sentiment_trend), 2) as avg_sentiment_trend,
    SUM(ticket_count) as total_tickets,
    ROUND(AVG(ticket_count), 1) as avg_tickets_per_customer,
    COUNT(CASE WHEN overall_sentiment = 'Negative' THEN 1 END) as negative_sentiment_count,
    COUNT(CASE WHEN overall_sentiment = 'Positive' THEN 1 END) as positive_sentiment_count
FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS
GROUP BY churn_risk
ORDER BY total_tickets DESC, avg_sentiment_score ASC
LIMIT 10;

-- 3. 🎫 Support Ticket Heat Map - What's hot in the support queue?
SELECT 
    priority_level,
    COUNT(*) as ticket_count,
    ROUND(AVG(sentiment_score), 2) as avg_sentiment,
    COUNT(DISTINCT customer_id) as unique_customers
FROM ANALYTICS.FACT_SUPPORT_TICKETS
GROUP BY priority_level
ORDER BY 
    CASE priority_level
        WHEN 'Critical' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        WHEN 'Low' THEN 4
    END;

-- 4. 🗺️ Customer Journey Map - Plot those customer adventures
WITH customer_journey AS (
    SELECT 
        cps.customer_id,
        cps.derived_persona,
        cps.churn_risk,
        cps.upsell_opportunity,
        cps.avg_sentiment,
        cps.sentiment_trend,
        cps.ticket_count,
        cps.avg_rating,
        cps.customer_summary
    FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps
)
SELECT 
    derived_persona,
    churn_risk,
    upsell_opportunity,
    COUNT(*) as customer_count,
    ROUND(AVG(avg_sentiment), 2) as avg_sentiment,
    ROUND(AVG(avg_rating), 2) as avg_rating,
    ROUND(AVG(ticket_count), 2) as avg_tickets
FROM customer_journey
GROUP BY derived_persona, churn_risk, upsell_opportunity
ORDER BY customer_count DESC;

-- 5. 🌍 Global Customer Vibes - How's the love worldwide?
SELECT 
    review_language,
    COUNT(*) as review_count,
    ROUND(AVG(review_rating), 2) as avg_rating,
    ROUND(AVG(sentiment_score), 2) as avg_sentiment,
    COUNT(DISTINCT customer_id) as unique_customers
FROM ANALYTICS.FACT_PRODUCT_REVIEWS
GROUP BY review_language
ORDER BY review_count DESC;

-- 6. 🤝 Customer Touch Points - Where and how are they reaching out?
SELECT 
    interaction_type,
    COUNT(*) as interaction_count,
    ROUND(AVG(sentiment_score), 2) as avg_sentiment,
    COUNT(DISTINCT customer_id) as unique_customers
FROM ANALYTICS.FACT_CUSTOMER_INTERACTIONS
GROUP BY interaction_type
ORDER BY interaction_count DESC;

-- 7. 💰 Value vs. Vibes Analysis - Are our VIPs feeling the love?
SELECT 
    CASE 
        WHEN lifetime_value >= 1000 THEN 'High Value'
        WHEN lifetime_value >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as value_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(avg_sentiment), 2) as avg_sentiment,
    ROUND(AVG(ticket_count), 2) as avg_tickets,
    ROUND(AVG(avg_rating), 2) as avg_rating
FROM ANALYTICS.CUSTOMER_PERSONA_SIGNALS cps
JOIN ANALYTICS.CUSTOMER_BASE cb USING (customer_id)
GROUP BY value_segment
ORDER BY 
    CASE value_segment
        WHEN 'High Value' THEN 1
        WHEN 'Medium Value' THEN 2
        WHEN 'Low Value' THEN 3
    END;

-- Table comments
COMMENT ON TABLE ANALYTICS.CUSTOMER_BASE IS 'Core customer information containing demographic data and customer identifiers';
COMMENT ON TABLE ANALYTICS.FACT_CUSTOMER_INTERACTIONS IS 'Customer interaction events with timestamps, interaction types, and sentiment analysis scores';
COMMENT ON TABLE ANALYTICS.FACT_PRODUCT_REVIEWS IS 'Product reviews with ratings, review text, and sentiment analysis across multiple languages';
COMMENT ON TABLE ANALYTICS.FACT_SUPPORT_TICKETS IS 'Support tickets with priority levels, resolution times, and customer satisfaction metrics';
COMMENT ON TABLE ANALYTICS.SENTIMENT_TRENDS IS 'Aggregated customer sentiment metrics including trends, volatility, and average sentiment scores';
COMMENT ON TABLE ANALYTICS.INSIGHT_SUMMARIES IS 'Cortex LLM function summaries of customer behavior and preferences for business insights';
COMMENT ON TABLE ANALYTICS.TICKET_PATTERNS IS 'Patterns and trends in customer support tickets including categorization and priority distribution';
COMMENT ON TABLE ANALYTICS.CUSTOMER_PERSONA_SIGNALS IS 'Customer segmentation data with derived personas, churn risk, and upsell opportunity indicators';

-- Add column comments for STG_CUSTOMER_INTERACTIONS
COMMENT ON COLUMN ANALYTICS.STG_CUSTOMER_INTERACTIONS.interaction_id IS 'Unique identifier for each customer interaction';
COMMENT ON COLUMN ANALYTICS.STG_CUSTOMER_INTERACTIONS.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.STG_CUSTOMER_INTERACTIONS.interaction_date IS 'Timestamp when the interaction occurred';
COMMENT ON COLUMN ANALYTICS.STG_CUSTOMER_INTERACTIONS.agent_id IS 'Unique identifier for the customer service agent';
COMMENT ON COLUMN ANALYTICS.STG_CUSTOMER_INTERACTIONS.interaction_type IS 'Type of customer interaction (e.g., call, email, chat)';
COMMENT ON COLUMN ANALYTICS.STG_CUSTOMER_INTERACTIONS.interaction_notes IS 'Detailed notes or transcript of the interaction';

-- Add column comments for STG_PRODUCT_REVIEWS
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.review_id IS 'Unique identifier for each product review';
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.customer_id IS 'Unique identifier for the customer who wrote the review';
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.product_id IS 'Unique identifier for the reviewed product';
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.review_date IS 'Timestamp when the review was posted';
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.review_rating IS 'Numeric rating given by the customer (1-5)';
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.review_text IS 'Text content of the product review';
COMMENT ON COLUMN ANALYTICS.STG_PRODUCT_REVIEWS.review_language IS 'Language code of the review text';

-- Add column comments for STG_SUPPORT_TICKETS
COMMENT ON COLUMN ANALYTICS.STG_SUPPORT_TICKETS.ticket_id IS 'Unique identifier for each support ticket';
COMMENT ON COLUMN ANALYTICS.STG_SUPPORT_TICKETS.customer_id IS 'Unique identifier for the customer who created the ticket';
COMMENT ON COLUMN ANALYTICS.STG_SUPPORT_TICKETS.ticket_date IS 'Timestamp when the ticket was created';
COMMENT ON COLUMN ANALYTICS.STG_SUPPORT_TICKETS.ticket_status IS 'Current status of the support ticket';
COMMENT ON COLUMN ANALYTICS.STG_SUPPORT_TICKETS.ticket_category IS 'Category classification of the support ticket';
COMMENT ON COLUMN ANALYTICS.STG_SUPPORT_TICKETS.ticket_description IS 'Detailed description of the support issue';

-- Add column comments for CUSTOMER_BASE
COMMENT ON COLUMN ANALYTICS.CUSTOMER_BASE.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_BASE.persona IS 'Customer persona classification';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_BASE.sign_up_date IS 'Date when the customer first registered';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_BASE.products_owned IS 'Number of products owned by the customer';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_BASE.lifetime_value IS 'Total lifetime value of the customer';

-- Add column comments for FACT_CUSTOMER_INTERACTIONS
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.interaction_id IS 'Unique identifier for each customer interaction';
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.interaction_date IS 'Timestamp when the interaction occurred';
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.agent_id IS 'Unique identifier for the customer service agent';
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.interaction_type IS 'Type of customer interaction (e.g., call, email, chat)';
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.interaction_notes IS 'Detailed notes or transcript of the interaction';
COMMENT ON COLUMN ANALYTICS.FACT_CUSTOMER_INTERACTIONS.sentiment_score IS 'Cortex LLM function sentiment score for the interaction (-1 to 1)';

-- Add column comments for FACT_PRODUCT_REVIEWS
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.review_id IS 'Unique identifier for each product review';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.customer_id IS 'Unique identifier for the customer who wrote the review';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.product_id IS 'Unique identifier for the reviewed product';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.review_date IS 'Timestamp when the review was posted';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.review_rating IS 'Numeric rating given by the customer (1-5)';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.review_text IS 'Text content of the product review';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.review_language IS 'Language code of the review text';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.sentiment_score IS 'Cortex LLM function sentiment score for the review (-1 to 1)';
COMMENT ON COLUMN ANALYTICS.FACT_PRODUCT_REVIEWS.review_text_english IS 'English translation of the review text';

-- Add column comments for FACT_SUPPORT_TICKETS
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.ticket_id IS 'Unique identifier for each support ticket';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.customer_id IS 'Unique identifier for the customer who created the ticket';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.ticket_date IS 'Timestamp when the ticket was created';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.ticket_status IS 'Current status of the support ticket';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.ticket_category IS 'Category classification of the support ticket';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.ticket_description IS 'Detailed description of the support issue';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.sentiment_score IS 'Cortex LLM function sentiment score for the ticket (-1 to 1)';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.priority_level IS 'Cortex-classified priority level (Critical, High, Medium, Low)';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.expected_resolution_timeframe IS 'Cortex-extracted customer expected resolution timeframe';
COMMENT ON COLUMN ANALYTICS.FACT_SUPPORT_TICKETS.requested_remedy IS 'Cortex-extracted customer requested compensation or remedy';

-- Add column comments for SENTIMENT_ANALYSIS
COMMENT ON COLUMN ANALYTICS.SENTIMENT_ANALYSIS.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_ANALYSIS.interaction_date IS 'Timestamp of the interaction';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_ANALYSIS.sentiment_score IS 'Cortex LLM function sentiment score (-1 to 1)';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_ANALYSIS.source_type IS 'Type of interaction (interaction, review, ticket)';

-- Add column comments for SENTIMENT_TRENDS
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.sentiment_history IS 'Array of sentiment scores over time';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.avg_sentiment IS 'Average sentiment score across all interactions';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.min_sentiment IS 'Lowest sentiment score recorded';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.max_sentiment IS 'Highest sentiment score recorded';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.sentiment_volatility IS 'Range between min and max sentiment scores';
COMMENT ON COLUMN ANALYTICS.SENTIMENT_TRENDS.sentiment_trend IS 'Change in sentiment from first to last interaction';

-- Add column comments for TICKET_PATTERNS
COMMENT ON COLUMN ANALYTICS.TICKET_PATTERNS.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.TICKET_PATTERNS.ticket_count IS 'Total number of support tickets';
COMMENT ON COLUMN ANALYTICS.TICKET_PATTERNS.first_ticket_date IS 'Date of first support ticket';
COMMENT ON COLUMN ANALYTICS.TICKET_PATTERNS.most_recent_ticket_date IS 'Date of most recent support ticket';
COMMENT ON COLUMN ANALYTICS.TICKET_PATTERNS.ticket_categories IS 'Array of ticket categories in chronological order';
COMMENT ON COLUMN ANALYTICS.TICKET_PATTERNS.ticket_priorities IS 'Array of ticket priorities in chronological order';

-- Add column comments for INSIGHT_SUMMARIES
COMMENT ON COLUMN ANALYTICS.INSIGHT_SUMMARIES.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.INSIGHT_SUMMARIES.customer_summary IS 'Cortex LLM function summary of customer interactions';

-- Add column comments for CUSTOMER_PERSONA_SIGNALS
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.customer_id IS 'Unique identifier for the customer';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.avg_sentiment IS 'Average sentiment score across all interactions';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.sentiment_trend IS 'Overall trend in sentiment over time';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.sentiment_volatility IS 'Variability in sentiment scores';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.overall_sentiment IS 'Categorized sentiment (Positive, Neutral, Negative)';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.ticket_count IS 'Total number of support tickets';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.ticket_categories IS 'Array of ticket categories';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.ticket_priorities IS 'Array of ticket priorities';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.avg_rating IS 'Average product review rating';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.customer_summary IS 'Cortex LLM function summary of customer interactions';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.derived_persona IS 'Cortex-classified customer persona type';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.churn_risk IS 'Predicted risk of customer churn (High, Medium, Low)';
COMMENT ON COLUMN ANALYTICS.CUSTOMER_PERSONA_SIGNALS.upsell_opportunity IS 'Predicted opportunity for upselling (High, Medium, Low)';

-- 🎉 Final Setup - Almost there!
CREATE OR REPLACE STAGE SEMANTIC_MODELS.CUSTOMER_INSIGHTS;