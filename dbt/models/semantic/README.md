# Semantic Models Directory

This directory contains semantic views and related artifacts for enabling natural language queries through Snowflake Cortex Analyst.

## Files in this directory

- **`customer_intelligence_semantic_view.sql`**: Main semantic view definition
- **`test_semantic_view.sql`**: Comprehensive validation queries demonstrating usage patterns
- **`schema.yml`**: DBT schema documentation for semantic models
- **`README.md`**: This file

## Semantic View Overview

The `customer_intelligence_semantic_view` provides a comprehensive business model for customer analytics, enabling both SQL queries and natural language queries through Cortex Analyst.

### Business Entities

- **customers**: Core customer profiles with personas, sentiment, and behavior signals
- **interactions**: Customer service interactions with sentiment analysis
- **reviews**: Product reviews with ratings, sentiment, and translations  
- **tickets**: Support tickets with priority classification and sentiment analysis

### Key Metrics Available

- **Customer Metrics**: Total customers, lifetime value, churn risk, sentiment scores
- **Interaction Metrics**: Total interactions, sentiment analysis, channel effectiveness
- **Review Metrics**: Total reviews, ratings, sentiment by language
- **Support Metrics**: Ticket volumes, priority distribution, resolution metrics

## DBT Integration

### Configuration

The semantic view is configured in `dbt_project.yml`:

```yaml
semantic:
  +schema: SEMANTIC_MODELS
  +materialized: table
```

### Variables Used

- `dbt_cortex_database`: Target database (default: DBT_CORTEX_LLMS)
- `semantic_schema`: Target schema (default: SEMANTIC_MODELS)
- `analytics_schema`: Source schema for fact tables (default: ANALYTICS)

### Creating/Updating Semantic Views

We provide three deployment methods:

#### Method 1: DBT Run (Recommended)
```bash
# Deploy like any other DBT model using our custom materialization
dbt run --select customer_intelligence_semantic_view
```

#### Method 2: DBT Compile + Snowflake CLI  
```bash
# Step 1: Compile the DBT template to resolve variables
dbt compile --select customer_intelligence_semantic_view

# Step 2: Execute the compiled SQL with Snowflake CLI
snow sql -f dbt/target/compiled/dbt_cortex/models/semantic/customer_intelligence_semantic_view.sql
```

#### Method 3: Direct Macro Deployment
```bash
# Deploy with the macro (for SQL without DBT templating)
dbt run-operation create_semantic_view --args '{semantic_view_name: "customer_intelligence_semantic_view", semantic_view_sql: "CREATE OR REPLACE SEMANTIC VIEW..."}'
```

### Dependencies

The semantic view depends on the following models:
- `customer_persona_signals` (analysis layer)
- `fact_customer_interactions` (fact layer)
- `fact_product_reviews` (fact layer)
- `fact_support_tickets` (fact layer)

Ensure these models are built before creating the semantic view:

```bash
# Build all dependencies first
dbt run --select models/analysis/customer_persona_signals models/fact/

# Then deploy the semantic view
dbt run --select customer_intelligence_semantic_view
```

## 🔍 Understanding Semantic View Granularity Rules

**IMPORTANT**: Semantic views have granularity rules that restrict which dimensions and metrics can be combined in a single query.

### ❌ Invalid Queries (Mixed Granularity)

```sql
-- ❌ Cannot mix customer and interaction metrics
SELECT * FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view
    DIMENSIONS interaction_type, priority_level  -- Different tables
    METRICS total_interactions, total_tickets     -- Different granularities
);
-- Error: Invalid dimension specified: The dimension entity 'TICKETS' must be related to and have an equal or lower level of granularity
```

### 💡 Granularity Levels in Our Model

1. **Customer Level** (Highest): customer_persona, customer_churn_risk, etc.
2. **Interaction Level**: interaction_type, interaction_date, etc.
3. **Review Level**: review_language, review_date, etc.
4. **Ticket Level**: priority_level, ticket_category, etc.

## 📊 SQL Query Examples

## 1. Complex Customer Segmentation Analysis
**Ask Cortex Analyst**: *"Can you break down our customers by their personas and show me who's at risk of churning versus who we could upsell to?"*

```bash
snow sql -q """
SELECT * FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view 
    DIMENSIONS customer_persona, customer_churn_risk, customer_upsell_opportunity 
    METRICS total_customers, average_lifetime_value, high_churn_risk_customers, high_upsell_opportunity_customers
) ORDER BY total_customers DESC;
"""
```

## 2. Advanced Temporal Channel Analysis
**Ask Cortex Analyst**: *"How has customer sentiment changed over time across our different communication channels?"*

```bash
snow sql -q """
SELECT * FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view 
    DIMENSIONS interaction_year, interaction_month, interaction_type 
    METRICS total_interactions, average_interaction_sentiment
) ORDER BY interaction_year, interaction_month, total_interactions DESC;
"""
```

## 3. Sophisticated Support Operations Intelligence
**Ask Cortex Analyst**: *"What's the story with our support tickets - how are they distributed by urgency and type, and how do customers feel about them?"*

```bash
snow sql -q """
SELECT * FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view 
    DIMENSIONS priority_level, ticket_category, ticket_status 
    METRICS total_tickets, critical_tickets, high_priority_tickets, average_ticket_sentiment
) ORDER BY total_tickets DESC 
LIMIT 20;
"""
```

## 4. International Product Review Analysis
**Ask Cortex Analyst**: *"How do product reviews look across different languages and years - are ratings and sentiment consistent globally?"*

```bash
snow sql -q """
SELECT * FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view 
    DIMENSIONS review_language, review_year 
    METRICS total_reviews, average_review_rating, average_review_sentiment, total_review_customers
) ORDER BY total_reviews DESC;
"""
```

## 5. Ultimate Executive Dashboard Query
**Ask Cortex Analyst**: *"Which of our substantial customer segments have the highest value and are most likely to stick around?"*

```bash
snow sql -q """
SELECT 
    customer_persona, 
    customer_churn_risk, 
    customer_overall_sentiment, 
    total_customers, 
    average_lifetime_value, 
    high_churn_risk_customers, 
    high_upsell_opportunity_customers 
FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view 
    DIMENSIONS customer_persona, customer_churn_risk, customer_overall_sentiment 
    METRICS total_customers, average_lifetime_value, high_churn_risk_customers, high_upsell_opportunity_customers
) 
WHERE total_customers > 10 
ORDER BY average_lifetime_value DESC, total_customers DESC;
"""
```

## ✅ Validation & Testing

### 1. Check Deployment
```sql
-- Verify the semantic view exists
SHOW SEMANTIC VIEWS IN SCHEMA DBT_CORTEX_LLMS.SEMANTIC_MODELS;

-- Get detailed structure
DESCRIBE SEMANTIC VIEW DBT_CORTEX_LLMS.SEMANTIC_MODELS.CUSTOMER_INTELLIGENCE_SEMANTIC_VIEW;
```

### 2. Basic Functionality Test
```sql
-- Simple metric test
SELECT * FROM SEMANTIC_VIEW(
    DBT_CORTEX_LLMS.SEMANTIC_MODELS.customer_intelligence_semantic_view
    METRICS total_customers
);
-- Expected result: 1000 customers
```

### 3. Run Comprehensive Tests
```bash
# Compile the test file
dbt compile --select test_semantic_view

# Run individual tests from the compiled file
snow sql -f dbt/target/compiled/dbt_cortex/models/semantic/test_semantic_view.sql
```

### 4. Test with Cortex Analyst
- Open Snowsight
- Navigate to AI & ML > Cortex Analyst
- Select the semantic view
- Try the example questions listed above

## 🚀 Creating New Semantic Views

### Step 1: Create the SQL File
```sql
-- models/semantic/my_semantic_view.sql
{{
  config(
    materialized='semantic_view'
  )
}}

CREATE OR REPLACE SEMANTIC VIEW {{ var('dbt_cortex_database') }}.{{ var('semantic_schema') }}.my_semantic_view

  TABLES (
    my_table AS {{ var('dbt_cortex_database') }}.{{ var('analytics_schema') }}.MY_TABLE
      PRIMARY KEY (id)
      WITH SYNONYMS ('my data', 'business entity')
      COMMENT = 'Description of what this table represents'
  )

  RELATIONSHIPS (
    -- Define relationships between tables
  )

  FACTS (
    -- Define row-level numerical facts
  )

  DIMENSIONS (
    -- Define categorical attributes with synonyms
    my_table.category AS category
      WITH SYNONYMS ('type', 'classification')
      COMMENT = 'Business description of this dimension'
  )

  METRICS (
    -- Define aggregated business metrics
    my_table.total_count AS COUNT(id)
      COMMENT = 'Total count of records'
  )

  COMMENT = 'Business description of the semantic view'
```

### Step 2: Deploy
```bash
# Deploy like any other DBT model
dbt run --select my_semantic_view
```

### Step 3: Test & Document
```bash
# Test basic functionality
snow sql -q "SELECT * FROM SEMANTIC_VIEW(schema.my_semantic_view METRICS total_count)"

# Add to schema.yml and update this README
```

## 🔧 Troubleshooting

### Common Issues

1. **"Semantic view not found"**
   - Check deployment: `SHOW SEMANTIC VIEWS IN SCHEMA schema_name`
   - Verify permissions: Ensure CREATE SEMANTIC VIEW privileges

2. **"Invalid dimension specified"**
   - Check granularity rules: Don't mix dimensions from different table levels
   - Use dimensions and metrics from the same logical table

3. **"Column not found"**
   - Verify source tables exist: `DESCRIBE TABLE source_table`
   - Check column names match exactly (case-sensitive)

4. **DBT compilation errors**
   - Verify variables are set: `dbt debug`
   - Check syntax in the .sql file

### Best Practices

1. **Granularity Awareness**: Always query dimensions and metrics at the same granularity level
2. **Business-Friendly Names**: Use synonyms that match how users naturally speak
3. **Rich Comments**: Provide context that helps Cortex Analyst understand business meaning
4. **Test Thoroughly**: Validate both SQL queries and natural language questions
5. **Version Control**: Track changes like any other DBT model
6. **Documentation**: Keep examples current and add new use cases

## 📚 Additional Resources

- [Snowflake Semantic Views Documentation](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [DBT Documentation](https://docs.getdbt.com/)

---