# Customer Intelligence Hub Quickstart

A comprehensive customer analytics platform that leverages Snowflake Cortex AI functions, dbt transformations, and interactive Streamlit dashboards to unlock deep insights from customer data across multiple touchpoints.

## 📊 Overview

The Customer Intelligence Hub demonstrates how AI-powered analytics can transform customer understanding across departments:

- **🧠 AI-Powered Insights**: Uses Snowflake Cortex LLM functions to extract sentiment, classify personas, and analyze unstructured text
- **🔄 Modern Data Stack**: Built with dbt for transformations, Snowflake for data warehousing, and Streamlit for visualization
- **🌐 Multi-language Support**: Processes customer feedback in multiple languages with automatic translation
- **📈 Cross-Department Value**: Provides actionable insights for Marketing, Sales, Support, Finance, and HR teams

## 🏗️ Project Structure

```
dbt_cortex_analytics/
├── dbt/                          # dbt project for data transformations
│   ├── models/
│   │   ├── staging/             # Raw data cleaning and staging
│   │   ├── fact/                # Core business entity tables
│   │   └── analysis/            # AI-enhanced analytics models
│   ├── seeds/                   # Sample data files
│   └── dbt_project.yml         # dbt configuration
├── streamlit/                   # Interactive dashboard application
│   ├── src/
│   │   ├── components/         # Dashboard components
│   │   ├── sql/               # SQL queries for dashboards
│   │   ├── utils/             # Utility functions
│   │   └── streamlit_app.py   # Main application
│   └── cortex_analyst/        # Cortex Analyst semantic model
├── data/                       # Sample datasets and generator
├── snowflake_sql/             # Snowflake-specific SQL scripts
└── docs/                      # Documentation and diagrams
```

## 🎯 Key Features

### Customer Intelligence Dashboard
- **📊 Overview**: KPIs, sentiment trends, and churn risk analysis
- **🎯 Segmentation**: Customer persona analysis and value segmentation
- **💭 Sentiment Experience**: Cross-channel sentiment tracking and recovery metrics
- **📝 Product Feedback**: Review analysis with multi-language support
- **🎧 Support Operations**: Ticket analytics and agent performance metrics
- **🤖 Cortex Analyst**: Natural language querying of customer data

### AI-Powered Analytics
- **Sentiment Analysis**: Emotional insights from customer interactions, reviews, and support tickets
- **Persona Classification**: Automatic customer segmentation based on behavior patterns
- **Multilingual Processing**: Supports customer feedback in English, Spanish, French, German, Italian, and Portuguese
- **Predictive Insights**: Churn risk scoring and upsell opportunity identification
- **Natural Language Generation**: AI-generated summaries and insights

### Data Pipeline
- **Staging Layer**: Clean and structure raw JSON data
- **Fact Layer**: Core business entities with enriched attributes
- **Analytics Layer**: AI-enhanced models with sentiment scores, persona signals, and trend analysis

## 🚀 Quick Start

### Prerequisites

- Snowflake account with Cortex LLM functions enabled
- dbt Cloud account (or dbt Core installed locally)
- Python 3.8+ for Streamlit application
- Git for version control

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd dbt_cortex_analytics

# Set up Python environment for Streamlit
conda create -n py311 python=3.11
conda activate py311
pip install -r streamlit/src/requirements.txt

# Install dbt (if using locally)
pip install dbt-core dbt-snowflake
```

### 2. Snowflake Configuration

1. Create database and schemas:
```sql
-- Run the setup script
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS DBT_CORTEX_LLMS;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.STAGE;
CREATE SCHEMA IF NOT EXISTS DBT_CORTEX_LLMS.ANALYTICS;
```

2. Configure dbt profiles (`~/.dbt/profiles.yml`):
```yaml
dbt_cortex:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-account>
      user: <your-username>
      private_key_path: <path-to-private-key>
      database: DBT_CORTEX_LLMS
      warehouse: <your-warehouse>
      schema: ANALYTICS
      role: <your-role>
```

### 3. Data Pipeline Execution

```bash
# Navigate to dbt directory
cd dbt

# Install dependencies and run models
dbt deps
dbt seed
dbt run
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### 4. Deploy Streamlit Application

```bash
# Deploy to Snowflake (recommended)
cd streamlit
./deploy_streamlit_to_snowflake.sh

# Or run locally
cd src
streamlit run streamlit_app.py
```

## 📊 Data Model

### Source Data
- **Customer Interactions**: Notes from customer service interactions with sentiment analysis
- **Product Reviews**: Multi-language product feedback with ratings and sentiment
- **Support Tickets**: Customer support requests with categorization and priority scoring
- **Customer Profiles**: Demographics, persona classification, and lifetime value

### Key Models
- `CUSTOMER_PERSONA_SIGNALS`: Comprehensive customer profiles with AI-derived insights
- `FACT_CUSTOMER_INTERACTIONS`: Interactions enriched with sentiment scores
- `FACT_PRODUCT_REVIEWS`: Reviews with sentiment analysis and translations
- `FACT_SUPPORT_TICKETS`: Support data with AI-powered categorization
- `SENTIMENT_ANALYSIS`: Cross-channel sentiment tracking and trends

## 🎨 Streamlit Dashboard Components

### 📈 Overview Dashboard
- Customer satisfaction KPIs
- Sentiment distribution and trends
- Churn risk analysis
- Interaction volume metrics

### 🎯 Customer Segmentation
- Persona-based analysis
- Value segment performance
- Churn vs. upsell opportunities
- Behavioral clustering

### 💭 Sentiment Experience
- Cross-channel sentiment alignment
- Recovery rate tracking
- Volatility analysis
- Persona-specific insights

### 📝 Product Feedback
- Rating trends and distribution
- Language-specific analysis
- Recent review highlights
- Sentiment correlation

### 🎧 Support Operations
- Ticket volume and trends
- Channel effectiveness
- Resolution metrics
- Agent performance

### 🤖 Cortex Analyst
- Natural language querying
- AI-generated insights
- Interactive data exploration
- Semantic model integration

## 🛠️ Technical Architecture

### Data Stack
- **Data Warehouse**: Snowflake with Cortex AI functions
- **Transformation**: dbt for SQL-based transformations
- **Visualization**: Streamlit with interactive charts and metrics
- **AI/ML**: Snowflake Cortex for sentiment analysis and text processing

### Key Technologies
- **Snowflake Cortex**: SENTIMENT, TRANSLATE, COMPLETE functions
- **dbt**: Data modeling, testing, and documentation
- **Streamlit**: Interactive web application framework
- **Plotly**: Advanced data visualizations
- **Pandas**: Data manipulation and analysis

## 📚 Sample Data

The project includes synthetic customer data that demonstrates realistic business scenarios:

- **Customer Personas**: Satisfied, frustrated, neutral, mixed behavior patterns
- **Multilingual Reviews**: Product feedback in 6 languages
- **Interaction Patterns**: Correlated sentiment across touchpoints
- **Support Scenarios**: Various ticket types and resolution patterns

### Generating Custom Data

```bash
cd data/generator
python generate_synthetic_data.py --num-records 100
```

## 🔒 Security & Best Practices

- Use private key authentication for Snowflake connections
- Store sensitive configuration in environment variables
- Implement role-based access controls
- Exclude credentials from version control
- Use Snowflake's secure data sharing capabilities

## 📖 Documentation

- **Architecture**: See `docs/erd.png` for entity relationship diagram
- **Data Lineage**: See `docs/dbt-dag.png` for transformation flow
- **Quickstart Guide**: See `snowflake-dbt-cortex-llms-quickstart.md`
- **Streamlit Docs**: See `streamlit/README.md`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample data
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions and support:
- Check the quickstart guide for detailed setup instructions
- Review dbt documentation for model development
- Consult Snowflake Cortex documentation for AI function usage
- Open GitHub issues for bugs and feature requests

---

**Powered by Snowflake ❄️ and dbt 🧱** 
