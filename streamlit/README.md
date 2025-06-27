# Customer Intelligence Hub

An advanced customer analytics platform that leverages **Snowflake Cortex AI**, **dbt transformations** to unlock deep insights from customer data across multiple touchpoints. Features natural language querying through Cortex Analyst and intelligent visualization recommendations.

## 🌟 Key Features

### 🧠 **AI-Powered Analytics**
- **Cortex Analyst Integration**: Natural language querying with conversational AI
- **Dynamic Chart Intelligence**: Automatic visualization recommendations
- **Sentiment Analysis**: Multi-language sentiment processing using Snowflake Cortex
- **Intelligent Insights**: AI-generated summaries and recommendations

### 📊 **Interactive Dashboards**
- **Overview**: KPIs, sentiment trends, and churn risk analysis
- **Sentiment & Experience**: Cross-channel sentiment tracking and recovery metrics
- **Support Operations**: Ticket analytics and agent performance metrics
- **Product Feedback**: Multi-language review analysis with intelligent charts
- **Customer Segmentation**: Persona-based analysis and value segmentation
- **Cortex Analyst**: Natural language data exploration with smart visualizations

### 🛡️ **Enterprise Security**
- **Dual Authentication**: Private key authentication for database connections, PAT token for API calls
- **Secure Token Storage**: File-based credential management with proper permissions
- **Role-Based Access**: Snowflake role and warehouse management

### ♿ **Accessibility & UX**
- **Dynamic Theming**: Light/dark mode with system preference detection
- **Responsive Design**: Optimized for desktop and mobile viewing
- **Debug Mode**: Comprehensive development and troubleshooting tools

## 🏗️ Project Structure

```
streamlit/
├── cortex_analyst/
│   ├── semantic_model.yaml       # Cortex Analyst semantic model definition
│   └── upload_semantic_model.sh  # Script to deploy semantic model
├── src/
│   ├── streamlit_app.py          # Main Streamlit application entry point
│   ├── .streamlit/
│   │   └── secrets.toml.example  # Template with setup instructions
│   ├── assets/                   # Static files (images, CSS, logos)
│   │   ├── styles.css           # Custom CSS styling
│   │   ├── snowflake-logo.png   # Snowflake branding
│   │   └── dbt-labs-*.svg       # dbt branding assets
│   ├── components/               # Dashboard components (one per tab)
│   │   ├── __init__.py          # Component registry system
│   │   ├── overview.py          # Overview dashboard with KPIs
│   │   ├── sentiment_experience.py # Sentiment analysis and trends
│   │   ├── support_ops.py       # Support operations analytics
│   │   ├── product_feedback.py  # Product review analysis
│   │   ├── segmentation.py      # Customer segmentation
│   │   └── cortex_analyst.py    # Natural language querying interface
│   ├── sql/                     # SQL queries organized by dashboard
│   │   ├── overview/            # Overview dashboard queries
│   │   ├── sentiment_experience/ # Sentiment analysis queries
│   │   ├── support_ops/         # Support operations queries
│   │   ├── product_feedback/    # Product feedback queries
│   │   └── segmentation/        # Segmentation queries
│   ├── utils/                   # Utility functions and modules
│   │   ├── __init__.py          # Utility exports
│   │   ├── database.py          # Snowflake connector and query execution
│   │   ├── chart_intelligence.py # Dynamic chart recommendation engine
│   │   ├── chart_factory.py     # Intelligent visualization generation
│   │   ├── accessibility.py     # WCAG 2.1 compliance utilities
│   │   ├── theme.py             # Dynamic theming system
│   │   ├── kpi_cards.py         # KPI visualization components
│   │   ├── auth.py              # Authentication utilities
│   │   ├── debug.py             # Debug mode and development tools
│   │   └── utils.py             # General utility functions
│   ├── requirements.txt         # Python dependencies
│   ├── environment.yml          # Conda environment specification
│   └── test_*.py                # Comprehensive test suites
├── deploy_streamlit_to_snowflake.sh # Deployment script for Snowflake
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites
- **Snowflake Account** with Cortex AI functions enabled
- **Python 3.8+** or Conda environment
- **Private Key Pair** for secure authentication
- **PAT Token** for Cortex Analyst API access

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd dbt_cortex_analytics/streamlit

# Option A: Using Conda (recommended)
conda create -n customer-analytics python=3.11
conda activate customer-analytics
pip install -r src/requirements.txt

# Option B: Using pip
pip install -r src/requirements.txt
```

### 2. Authentication Configuration

#### Private Key Setup (Database Connections)
```bash
# Generate RSA key pair
openssl genrsa -out rsa_key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in rsa_key.pem -out rsa_key.p8
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub

# Store securely
mkdir -p ~/.ssh/snowflake/keys
mv rsa_key.p8 ~/.ssh/snowflake/keys/
chmod 600 ~/.ssh/snowflake/keys/rsa_key.p8

# Add public key to Snowflake user
# ALTER USER <username> SET RSA_PUBLIC_KEY='<public_key_content>';
```

#### PAT Token Setup (Cortex Analyst API)
```bash
# Generate PAT token in Snowsight: Admin → Users & Roles → Generate Token
# Store securely
echo "your_pat_token_here" > ~/.ssh/snowflake/keys/pat_token.txt
chmod 600 ~/.ssh/snowflake/keys/pat_token.txt
```

### 3. Configuration

Copy and customize the secrets configuration:
```bash
cd src
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit secrets.toml with your Snowflake credentials and file paths
```

### 4. Run the Application

```bash
cd src
streamlit run streamlit_app.py
```

## 🎯 Advanced Features

### 🗣️ Cortex Analyst Integration
- **Natural Language Queries**: Ask questions in plain English
- **Intelligent SQL Generation**: Automatic query generation with confidence scoring
- **Interactive Results**: Smart chart recommendations for query results
- **Conversation History**: Contextual follow-up questions and suggestions
- **Multi-Modal Responses**: Text answers, SQL queries, and visualizations

### 🎨 Accessibility & Theming
- **Dynamic Themes**: Automatic light/dark mode with system preference detection
- **Responsive Design**: Optimized for all screen sizes and devices

### 🧠 Dynamic Chart Intelligence
- **Automatic Analysis**: Detects data types, patterns, and statistical properties
- **Smart Recommendations**: Suggests optimal visualizations based on data characteristics
- **Confidence Scoring**: Provides reasoning and confidence levels for each recommendation
- **Transferable**: Works with any Snowflake semantic model and Cortex Analyst implementation

## 🗄️ Database Schema

The application connects to Snowflake tables in the `ANALYTICS` schema:

### Core Tables
- **`CUSTOMER_PERSONA_SIGNALS`**: Comprehensive customer profiles with AI-derived insights
- **`FACT_CUSTOMER_INTERACTIONS`**: Interactions enriched with sentiment scores
- **`FACT_PRODUCT_REVIEWS`**: Reviews with sentiment analysis and translations
- **`FACT_SUPPORT_TICKETS`**: Support data with AI-powered categorization
- **`SENTIMENT_ANALYSIS`**: Cross-channel sentiment tracking and trends

### AI Enhancements
All tables leverage **Snowflake Cortex** functions for:
- **Sentiment Analysis**: `SENTIMENT()` function for emotional insights
- **Translation**: `TRANSLATE()` for multi-language support
- **Classification**: `CLASSIFY_TEXT()` for automated categorization
- **Summarization**: `SUMMARIZE()` for insight generation

## 🔧 Development

### Debug Mode
Enable debug mode in the application sidebar for:
- **Query Inspection**: View generated SQL and execution times
- **Data Profiling**: Detailed dataset analysis and statistics
- **Performance Metrics**: Component render times and optimization insights
- **Authentication Debugging**: Connection status and credential sourcing

### Custom Components
1. **Create Component**: Add new file in `src/components/`
2. **Add SQL Queries**: Create corresponding queries in `src/sql/`
3. **Register Component**: Update `src/components/__init__.py`

## 📦 Dependencies

### Core Dependencies
```
streamlit>=1.33              # Web application framework
snowflake-connector-python   # Database connectivity
snowflake-snowpark-python    # Advanced Snowflake integration
pandas                       # Data manipulation
numpy                        # Numerical computing
plotly>=5                    # Interactive visualizations
scikit-learn                 # Machine learning for chart intelligence
```

### Visualization & Analytics
```
altair>=5                    # Grammar of graphics
seaborn                      # Statistical visualizations
matplotlib                   # Additional plotting capabilities
scipy                        # Scientific computing
kaleido>=0.2.1              # Static image export
```

### UI & Accessibility
```
streamlit-extras             # Enhanced Streamlit components
streamlit-lottie            # Animation support
pydeck                      # 3D visualizations
```

## 🚀 Deployment

### Local Development
```bash
cd src
streamlit run streamlit_app.py
```

### Snowflake Deployment
```bash
# Deploy to Streamlit-in-Snowflake
./deploy_streamlit_to_snowflake.sh
```

# Chart Intelligence Test Questions

A comprehensive set of test questions designed to test different chart recommendation types in the Cortex Analyst interface. Each question is crafted to trigger specific chart types based on the data characteristics and the chart intelligence system.

## 📊 Single Column Analysis (Histogram/Box Plot Tests)

### Histogram Recommendations
1. **"Show me the distribution of customer lifetime values"**
   - Expected: Histogram (continuous numeric data)
   - Tests: Single numeric column distribution analysis

2. **"What's the distribution of sentiment scores across all customer interactions?"**
   - Expected: Histogram with statistical overlays
   - Tests: Continuous numeric distribution with outlier detection

3. **"Display the distribution of customer sentiment volatility"**
   - Expected: Histogram with mean/median lines
   - Tests: Statistical annotations and distribution shape analysis

### Box Plot Recommendations
4. **"Show me outliers in customer lifetime values"**
   - Expected: Box plot with outlier points
   - Tests: Outlier detection and quartile analysis

5. **"Analyze the spread of sentiment scores with quartile information"**
   - Expected: Box plot with statistical annotations
   - Tests: Quartile analysis and statistical summary

## 📈 Time Series Analysis (Line/Area Chart Tests)

### Line Chart Recommendations
6. **"Show customer interaction trends over time by day"**
   - Expected: Line chart (temporal + numeric)
   - Tests: Time series visualization with trend analysis

7. **"Display sentiment score changes over time for the past year"**
   - Expected: Line chart with trend line
   - Tests: Temporal analysis with correlation detection

8. **"Track support ticket volume trends by week"**
   - Expected: Line chart with markers
   - Tests: Time-based aggregation and trend visualization

### Area Chart Recommendations
9. **"Show the cumulative customer interaction volume over time"**
   - Expected: Area chart (emphasizes magnitude)
   - Tests: Cumulative temporal analysis

10. **"Display sentiment recovery patterns over time with area emphasis"**
    - Expected: Area chart for magnitude visualization
    - Tests: Time series with volume emphasis

## 📊 Categorical Analysis (Bar Chart Tests)

### Vertical Bar Chart Recommendations
11. **"Compare customer counts by persona type"**
    - Expected: Vertical bar chart (categorical + count)
    - Tests: Category frequency analysis

12. **"Show average lifetime value by customer persona"**
    - Expected: Vertical bar chart with value comparison
    - Tests: Categorical aggregation and comparison

13. **"Display support ticket counts by priority level"**
    - Expected: Vertical bar chart
    - Tests: Categorical counting and sorting

### Horizontal Bar Chart Recommendations
14. **"Compare average sentiment scores across different interaction types"**
    - Expected: Horizontal bar chart (many categories)
    - Tests: Category label readability optimization

15. **"Show customer lifetime value by derived persona categories"**
    - Expected: Horizontal bar chart for better label display
    - Tests: Long category name handling

## 🔍 Correlation Analysis (Scatter Plot Tests)

### Scatter Plot Recommendations
16. **"Analyze the relationship between customer lifetime value and average sentiment"**
    - Expected: Scatter plot with correlation coefficient
    - Tests: Numeric vs numeric correlation analysis

17. **"Show the correlation between sentiment volatility and customer lifetime value per customer"**
    - Expected: Scatter plot with trend line
    - Tests: Correlation detection and trend analysis

18. **"Explore the relationship between products owned and average rating"**
    - Expected: Scatter plot with statistical annotations
    - Tests: Two-variable relationship analysis

## 🥧 Composition Analysis (Pie Chart Tests)

### Pie Chart Recommendations
19. **"Show the composition of customers by churn risk level"**
    - Expected: Pie chart (categorical with few categories)
    - Tests: Composition visualization for limited categories

20. **"Display the breakdown of support tickets by priority level"**
    - Expected: Pie chart with percentages
    - Tests: Categorical composition with percentage display

21. **"Show customer distribution across upsell opportunity categories"**
    - Expected: Pie chart for 3-4 categories
    - Tests: Small cardinality categorical composition

## 🔥 Multi-Dimensional Analysis (Heatmap Tests)

### Heatmap Recommendations
22. **"Show correlation matrix between lifetime value, sentiment score, products owned, and average rating"**
    - Expected: Correlation heatmap
    - Tests: Multi-variable correlation visualization

23. **"Display the relationship matrix between all numeric customer metrics"**
    - Expected: Correlation heatmap with color coding
    - Tests: Comprehensive correlation analysis

## 📋 Detailed Data Analysis (Table Tests)

### Table View Recommendations
24. **"Show me detailed customer information including persona, churn risk, and all metrics"**
    - Expected: Table view (multi-dimensional data)
    - Tests: Comprehensive data display

25. **"List all customer interactions with full details and sentiment scores"**
    - Expected: Table with pagination
    - Tests: Large dataset table handling

### Production Considerations
- **Security**: Use private key authentication and secure token storage
- **Performance**: Enable query caching and optimize SQL queries
- **Monitoring**: Implement logging and error tracking
- **Scaling**: Consider Snowflake warehouse sizing for concurrent users

## 🔒 Security Best Practices

### Authentication
- ✅ **Private Key Authentication**: For database connections
- ✅ **PAT Token Authentication**: For API calls
- ✅ **File-Based Secrets**: Credentials stored in secure files with 600 permissions
- ✅ **Dual Authentication**: Separate auth methods for different purposes

### Data Protection
- ✅ **Role-Based Access**: Snowflake role and warehouse isolation
- ✅ **Query Parameterization**: Protection against SQL injection
- ✅ **Connection Encryption**: TLS encryption for all connections
- ✅ **Token Rotation**: Regular credential rotation capabilities

## 📚 Documentation
- **[Setup Guide](src/.streamlit/secrets.toml.example)**: Comprehensive configuration instructions
- **[Cortex Analyst Guide](cortex_analyst/)**: Semantic model setup and deployment
- **[API Documentation](src/utils/)**: Utility functions and component APIs

## 🤝 Contributing

1. **Fork the Repository**
2. **Create Feature Branch**: `git checkout -b feature/amazing-feature`
3. **Test Changes**: Run test suite and verify functionality
4. **Update Documentation**: Update README and component docs
5. **Submit Pull Request**: Include detailed description of changes

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions and support:
- **Setup Issues**: Check the [secrets.toml.example](src/.streamlit/secrets.toml.example) for detailed instructions
- **Authentication**: Verify private key and PAT token configuration
- **Chart Intelligence**: Enable debug mode to troubleshoot visualization recommendations
- **Cortex Analyst**: Ensure semantic model is properly deployed and accessible

---

**Powered by Snowflake ❄️ + dbt 🧱 + Streamlit 🎈**