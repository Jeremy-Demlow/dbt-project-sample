"""
Overview dashboard component showing key metrics and trends.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
import pandas as pd
from utils.database import snowflake_conn
from utils.kpi_cards import render_kpis
from utils.debug import display_debug_info
from utils.theme import get_current_theme
import os
from datetime import datetime, timedelta
import json
from decimal import Decimal
import numpy as np
from plotly.subplots import make_subplots

def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def read_sql_file(file_path: str) -> str:
    """Read and return the contents of a SQL file.
    
    Args:
        file_path: Path to the SQL file relative to src/sql/
        
    Returns:
        str: Contents of the SQL file
    """
    full_path = os.path.join("src", "sql", file_path)
    with open(full_path, "r") as f:
        return f.read()

@st.cache_data(ttl=300)
def load_kpi_data() -> pd.DataFrame:
    """Load and cache KPI data.
    
    Returns:
        pd.DataFrame: KPI data
    """
    kpi_query = "overview/kpis.sql"
    results = snowflake_conn.execute_query(kpi_query)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=kpi_query,
            params={},
            results=df,
            query_name="KPI Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_trend_data() -> pd.DataFrame:
    """Load and cache trend data.
    
    Returns:
        pd.DataFrame: Trend data
    """
    trend_query = "overview/sentiment_trend.sql"
    results = snowflake_conn.execute_query(trend_query)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=trend_query,
            params={},
            results=df,
            query_name="Sentiment Trend Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_distribution_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Load and cache distribution data.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: Distribution data
    """
    dist_query = "overview/sentiment_dist.sql"
    dist_params = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    results = snowflake_conn.execute_query(dist_query, dist_params)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=dist_query,
            params=dist_params,
            results=df,
            query_name="Sentiment Distribution Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_risk_data() -> pd.DataFrame:
    """Load and cache risk data.
    
    Returns:
        pd.DataFrame: Risk data
    """
    risk_query = "overview/churn_risk_breakdown.sql"
    results = snowflake_conn.execute_query(risk_query)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=risk_query,
            params={},
            results=df,
            query_name="Churn Risk Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_interaction_trend() -> pd.DataFrame:
    """Load and cache interaction trend data.
    
    Returns:
        pd.DataFrame: Interaction trend data
    """
    trend_query = "overview/interaction_trend.sql"
    results = snowflake_conn.execute_query(trend_query)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=trend_query,
            params={},
            results=df,
            query_name="Interaction Trend Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_risk_trend() -> pd.DataFrame:
    """Load and cache risk trend data.
    
    Returns:
        pd.DataFrame: Risk trend data
    """
    trend_query = "overview/risk_trend.sql"
    results = snowflake_conn.execute_query(trend_query)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=trend_query,
            params={},
            results=df,
            query_name="Risk Trend Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_rating_trend() -> pd.DataFrame:
    """Load and cache rating trend data.
    
    Returns:
        pd.DataFrame: Rating trend data
    """
    trend_query = "product_feedback/rating_trend.sql"
    results = snowflake_conn.execute_query(trend_query)
    df = pd.DataFrame(results)
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=trend_query,
            params={},
            results=df,
            query_name="Rating Trend Query"
        )
    
    return df

def calculate_delta(trend_series, is_count_metric=False):
    """Calculate the percentage change between the last two values.
    
    Args:
        trend_series: Series containing the trend data
        is_count_metric: If True, treat as a count metric (like total interactions)
                        and calculate absolute change instead of percentage
    """
    if trend_series is None or len(trend_series) < 2:
        return 0
    
    # For count metrics, we want to compare the last 7 days with the previous 7 days
    if is_count_metric:
        # Get the last 14 days of data
        last_14_days = trend_series.tail(14)
        if len(last_14_days) < 14:
            return 0
            
        # Calculate current week and previous week totals
        current_week = last_14_days.tail(7).sum()
        previous_week = last_14_days.head(7).sum()
        
        if previous_week == 0:
            return 0
            
        # Calculate percentage change
        return ((current_week - previous_week) / previous_week) * 100
    else:
        # For other metrics, compare last two values
        current = trend_series.iloc[-1]
        previous = trend_series.iloc[-2]
        
        if previous == 0:
            return 0
            
        return ((current - previous) / abs(previous)) * 100

def get_smoothed_trend_data(df, column_name, window=30):
    """Get smoothed trend data using a moving average.
    
    Args:
        df: DataFrame containing the trend data
        column_name: Name of the column to smooth
        window: Window size for moving average (default: 30 days)
        
    Returns:
        pd.Series: Smoothed trend data
    """
    if df is not None and column_name in df.columns:
        trend_series = df.set_index('DATE')[column_name]
        return trend_series.rolling(window=window, min_periods=1).mean()
    return None

# Get trend data safely
def get_trend_data(df, column_name):
    if df is not None and column_name in df.columns:
        return df.set_index('DATE')[column_name]
    return None

def render_overview(filters: dict, debug_mode: bool = False) -> None:
    """Render the Overview dashboard tab.
    
    Args:
        filters: Dictionary containing selected date range and personas
        debug_mode: Boolean flag to control visibility of debug information
    """
    # Page title and description
    st.markdown("""
    <style>
    h2 {
        padding: 0 !important;
        margin: 0 !important;
    }
    </style>
    <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem;">
        <h2>Customer Experience Overview</h2>
        <div class="tooltip">
            <span class="help-icon">?</span>
            <span class="tooltiptext">
                Track key customer experience metrics, sentiment trends, and churn risk across your customer base. 
                <br><br>
                Press Tab to navigate through the dashboard sections. Use Enter to expand/collapse sections.
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load all data first
    with st.spinner("Loading overview data..."):
        kpi_data = load_kpi_data()
        sentiment_trend = load_trend_data()
        interaction_trend = load_interaction_trend()
        risk_trend = load_risk_trend()
        rating_trend = load_rating_trend()
        risk_data = load_risk_data()
    
    # Display debug information for filters and all queries if debug mode is enabled
    if st.session_state.get('debug_mode', False):
        st.markdown("### Debug Information")
        
        # Show debug info for each query
        queries = [
            ("KPI Query", "overview/kpis.sql", {}, kpi_data),
            ("Sentiment Trend Query", "overview/sentiment_trend.sql", {}, sentiment_trend),
            ("Interaction Trend Query", "overview/interaction_trend.sql", {}, interaction_trend),
            ("Risk Trend Query", "overview/risk_trend.sql", {}, risk_trend),
            ("Rating Trend Query", "product_feedback/rating_trend.sql", {}, rating_trend),
            ("Churn Risk Query", "overview/churn_risk_breakdown.sql", {}, risk_data)
        ]
        
        for query_name, sql_file, params, results in queries:
            display_debug_info(
                sql_file_path=sql_file,
                params=params,
                results=results,
                query_name=query_name
            )
    
    # Key Metrics Section
    # Format KPIs with null checks, trend indicators, and sparklines
    kpis = [
        {
            "label": "Avg Sentiment",
            "value": f"{kpi_data['AVG_SENTIMENT'].iloc[0]:.2f}" if not kpi_data.empty and kpi_data['AVG_SENTIMENT'].iloc[0] is not None else "N/A",
            "delta": calculate_delta(get_trend_data(sentiment_trend, 'AVG_SENTIMENT')),
            "timeframe": "Week",
            "help": """
            Average sentiment score across all customer interactions.
            - Range: -1.0 (very negative) to 1.0 (very positive)
            - Based on natural language processing of customer feedback
            - Updated daily from all communication channels
            """,
            "trend_data": get_smoothed_trend_data(sentiment_trend, 'AVG_SENTIMENT')
        },
        {
            "label": "Total Interactions",
            "value": f"{kpi_data['TOTAL_INTERACTIONS'].iloc[0]:,}" if not kpi_data.empty and kpi_data['TOTAL_INTERACTIONS'].iloc[0] is not None else "N/A",
            "delta": calculate_delta(get_trend_data(interaction_trend, 'INTERACTION_COUNT'), is_count_metric=True),
            "timeframe": "Week",
            "help": """
            Total number of customer interactions analyzed.
            - Includes support tickets, reviews, and feedback
            - Covers all communication channels
            - Used to calculate engagement metrics
            - Week-over-week change shown as percentage
            """,
            "trend_data": get_smoothed_trend_data(interaction_trend, 'INTERACTION_COUNT')
        },
        {
            "label": "High Risk %",
            "value": f"{kpi_data['HIGH_RISK_PCT'].iloc[0]:.1f}%" if not kpi_data.empty and kpi_data['HIGH_RISK_PCT'].iloc[0] is not None else "N/A",
            "delta": calculate_delta(get_trend_data(risk_trend, 'HIGH_RISK_PCT')),
            "timeframe": "Week",
            "help": """
            Percentage of customers identified as high churn risk.
            - Based on sentiment analysis and engagement patterns
            - High risk: < 0.3 sentiment score
            - Medium risk: 0.3-0.6 sentiment score
            - Low risk: > 0.6 sentiment score
            """,
            "trend_data": get_smoothed_trend_data(risk_trend, 'HIGH_RISK_PCT')
        },
        {
            "label": "Avg Rating",
            "value": f"{kpi_data['AVG_RATING'].iloc[0]:.1f}" if not kpi_data.empty and kpi_data['AVG_RATING'].iloc[0] is not None else "N/A",
            "delta": calculate_delta(get_trend_data(rating_trend, 'AVG_RATING')),
            "timeframe": "Week",
            "help": """
            Average product rating from customer reviews.
            - Scale: 1-5 stars
            - Weighted by review recency
            - Excludes spam and duplicate reviews
            """,
            "trend_data": get_smoothed_trend_data(rating_trend, 'AVG_RATING')
        }
    ]
    
    # Render KPI cards with enhanced styling
    render_kpis(kpis, columns=4)
    
    # Add download button for KPI data at the bottom
    from utils.snowflake_compatibility import safe_download_button
    safe_download_button(
        data=json.dumps(kpi_data.to_dict(orient='records'), indent=2, default=decimal_to_float),
        filename="kpi_data.json",
        label="⇓ Download KPI Data",
        mime_type="application/json",
        help_text="Download the current KPI data as JSON",
        key="download_kpi_overview"
    )
    
    # Sentiment Analysis Section
    with st.expander("Sentiment Analysis", expanded=True):
        # Create two columns for the main charts
        trend_col, dist_col = st.columns(2)
        
        # Sentiment Trend Chart
        with trend_col:
            # Add chart customization options horizontally above the chart
            c1, c2, c3 = st.columns(3)
            with c1:
                show_moving_avg = st.checkbox("Show Moving Average", value=True, help="Toggle the 7-day moving average line")
            with c2:
                show_zero_line = st.checkbox("Show Zero Line", value=True, help="Toggle the zero reference line")
            with c3:
                show_area = st.checkbox("Show Area Chart", value=True, help="Toggle the area chart overlay")
            st.markdown('''
            <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
                <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Sentiment Trend</h3>
                <div class="tooltip">
                    <span class="help-icon">?</span>
                    <span class="tooltiptext">
                        Shows the average customer sentiment score over time. Use this chart to identify trends, spikes, or drops in customer sentiment, and to correlate with events or changes in your business.
                    </span>
                </div>
            </div>
            ''', unsafe_allow_html=True)
            if not sentiment_trend.empty:
                # Create main figure
                fig = go.Figure()
                
                # Add gradient background for positive/negative regions
                fig.add_shape(
                    type="rect",
                    x0=sentiment_trend["DATE"].min(),
                    x1=sentiment_trend["DATE"].max(),
                    y0=0,
                    y1=1,
                    fillcolor="rgba(0, 255, 0, 0.1)",
                    line=dict(width=0),
                    layer="below"
                )
                fig.add_shape(
                    type="rect",
                    x0=sentiment_trend["DATE"].min(),
                    x1=sentiment_trend["DATE"].max(),
                    y0=-1,
                    y1=0,
                    fillcolor="rgba(255, 0, 0, 0.1)",
                    line=dict(width=0),
                    layer="below"
                )
                
                # Add area chart if enabled
                if show_area:
                    fig.add_trace(go.Scatter(
                        x=sentiment_trend["DATE"],
                        y=sentiment_trend["AVG_SENTIMENT"],
                        fill='tozeroy',
                        fillcolor='rgba(31, 119, 180, 0.2)',
                        line=dict(color='rgba(31, 119, 180, 0)'),
                        name="Sentiment Area",
                        showlegend=False,
                        hoverinfo='skip'
                    ))
                
                # Add sentiment line with enhanced styling
                fig.add_trace(go.Scatter(
                    x=sentiment_trend["DATE"],
                    y=sentiment_trend["AVG_SENTIMENT"],
                    name="Daily Sentiment",
                    line=dict(color='#1f77b4', width=2, shape='spline'),
                    hovertemplate="<b>Date:</b> %{x}<br>" +
                                "<b>Sentiment:</b> %{y:.2f}<br>" +
                                "<b>Change:</b> %{customdata[0]:.1%}<br>" +
                                "<b>Category:</b> %{customdata[1]}<extra></extra>",
                    customdata=np.column_stack((
                        sentiment_trend["AVG_SENTIMENT"].pct_change(),
                        sentiment_trend["AVG_SENTIMENT"].apply(lambda x: 
                            "Very Negative" if x < -0.6 else
                            "Negative" if x < -0.2 else
                            "Neutral" if x < 0.2 else
                            "Positive" if x < 0.6 else
                            "Very Positive"
                        )
                    )),
                    mode='lines+markers',
                    marker=dict(
                        size=6,
                        line=dict(width=1, color='white'),
                        color='#1f77b4'
                    )
                ))
                
                # Add moving average line with enhanced styling
                if show_moving_avg:
                    fig.add_trace(go.Scatter(
                        x=sentiment_trend["DATE"],
                        y=sentiment_trend["MOVING_AVG_SENTIMENT"],
                        name="7-day Moving Avg",
                        line=dict(color='#ff7f0e', width=2, dash='dash', shape='spline'),
                        hovertemplate="<b>Date:</b> %{x}<br><b>Moving Avg:</b> %{y:.2f}<extra></extra>"
                    ))
                
                # Add zero line for reference
                if show_zero_line:
                    fig.add_hline(
                        y=0,
                        line_dash="dot",
                        line_color="gray",
                        line_width=1,
                        annotation_text="Neutral",
                        annotation_position="bottom right"
                    )
                
                # Add threshold lines
                fig.add_hline(
                    y=0.6,
                    line_dash="dot",
                    line_color="#3b82f6",
                    line_width=1,
                    annotation_text="Very Positive",
                    annotation_position="top right"
                )
                fig.add_hline(
                    y=-0.6,
                    line_dash="dot",
                    line_color="#3b82f6",
                    line_width=1,
                    annotation_text="Very Negative",
                    annotation_position="bottom right"
                )
                
                # Update layout with enhanced styling
                fig.update_layout(
                    title=dict(
                        text="",
                        x=0.5,
                        y=0.95,
                        xanchor='center',
                        yanchor='top',
                        font=dict(size=20)
                    ),
                    xaxis_title="Date",
                    yaxis_title="Sentiment Score",
                    hovermode="x unified",
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1,
                        bgcolor='rgba(255, 255, 255, 0.8)'
                    ),
                    height=500,
                    margin=dict(l=20, r=20, t=40, b=20),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(
                        showgrid=True,
                        gridcolor='rgba(128, 128, 128, 0.2)',
                        zeroline=False,
                        rangeslider=dict(visible=True),
                        rangeselector=dict(
                            buttons=list([
                                dict(count=1, label="1M", step="month", stepmode="backward"),
                                dict(count=3, label="3M", step="month", stepmode="backward"),
                                dict(count=6, label="6M", step="month", stepmode="backward"),
                                dict(count=1, label="1Y", step="year", stepmode="backward"),
                                dict(step="all")
                            ])
                        )
                    ),
                    yaxis=dict(
                        showgrid=True,
                        gridcolor='rgba(128, 128, 128, 0.2)',
                        zeroline=False,
                        range=[-1, 1]
                    ),
                    modebar=dict(
                        orientation="v",
                        bgcolor="rgba(255, 255, 255, 0.7)",
                        color="rgba(0, 0, 0, 0.5)",
                        activecolor="rgba(0, 0, 0, 0.7)"
                    )
                )
                
                # Display the main chart
                st.plotly_chart(fig, use_container_width=True, config={
                    'responsive': True,
                    'displayModeBar': True,
                    'scrollZoom': True,
                    'modeBarButtonsToAdd': ['drawline', 'eraseshape']
                })
            else:
                st.info("No sentiment trend data available for the selected filters.")
        
        # Sentiment Distribution Chart
        with dist_col:
            st.markdown('''
            <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
                <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Sentiment Distribution</h3>
                <div class="tooltip">
                    <span class="help-icon">?</span>
                    <span class="tooltiptext">
                        Visualizes the distribution of customer sentiment across categories (Very Negative to Very Positive) for the selected period. Use this to understand the proportion of positive, neutral, and negative feedback.
                    </span>
                </div>
            </div>
            ''', unsafe_allow_html=True)
            # Load distribution data
            sentiment_dist = load_distribution_data(
                filters['start_date'],
                filters['end_date']
            )
            
            if not sentiment_dist.empty:
                # Create tabs for different visualizations
                tab1, tab2, tab3 = st.tabs(["Bar Chart", "Bubble Chart", "Heatmap"])
                
                with tab1:
                    # Original bar chart
                    chart = alt.Chart(sentiment_dist).mark_bar().encode(
                        x=alt.X('SENTIMENT_BUCKET:N', 
                               sort=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive'],
                               title='Sentiment'),
                        y=alt.Y('PERCENTAGE:Q', title='Percentage'),
                        color=alt.Color('SENTIMENT_BUCKET:N', 
                                      sort=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive'],
                                      scale=alt.Scale(domain=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive'],
                                                    range=['#d62728', '#ff7f0e', '#bcbd22', '#2ca02c', '#1f77b4']))
                    )
                    
                    # Get current theme
                    theme = get_current_theme()
                    
                    chart = chart.properties(
                        height=400
                    ).configure_axis(
                        labelFontSize=12,
                        titleFontSize=14,
                        grid=False,
                        domainColor=theme['text'],
                        labelColor=theme['text'],
                        titleColor=theme['text']
                    ).configure_view(
                        strokeWidth=0,
                        fill=theme['background']
                    ).configure_title(
                        fontSize=16,
                        color=theme['text']
                    ).configure(
                        background=theme['background']
                    )
                    
                    st.altair_chart(chart, use_container_width=True)
                
                with tab2:
                    # Load sentiment distribution data
                    sentiment_dist = load_distribution_data(
                        filters['start_date'],
                        filters['end_date']
                    )
                    
                    # Bubble chart showing sentiment distribution
                    fig = go.Figure()
                    
                    # Get current theme
                    theme = get_current_theme()
                    
                    fig.add_trace(go.Scatter(
                        x=sentiment_dist["SENTIMENT_BUCKET"],
                        y=[1] * len(sentiment_dist),  # Place all bubbles on same y-level
                        mode='markers',
                        marker=dict(
                            size=sentiment_dist["COUNT"] / sentiment_dist["COUNT"].max() * 100,
                            color=sentiment_dist["PERCENTAGE"],
                            colorscale='RdYlGn',
                            showscale=True,
                            colorbar=dict(
                                title='Percentage',
                                tickfont=dict(color=theme['text'])
                            )
                        ),
                        text=sentiment_dist.apply(
                            lambda row: f"Sentiment: {row['SENTIMENT_BUCKET']}<br>Count: {row['COUNT']:,}<br>Percentage: {row['PERCENTAGE']:.1%}",
                            axis=1
                        ),
                        hoverinfo='text'
                    ))
                    
                    fig.update_layout(
                        xaxis_title="Sentiment Category",
                        height=400,
                        showlegend=False,
                        paper_bgcolor=theme['background'],
                        plot_bgcolor=theme['background'],
                        font=dict(color=theme['text']),
                        xaxis=dict(
                            gridcolor=theme['border'],
                            linecolor=theme['border'],
                            tickfont=dict(color=theme['text'])
                        ),
                        yaxis=dict(
                            showticklabels=False,
                            gridcolor=theme['border'],
                            linecolor=theme['border']
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                with tab3:
                    # Heatmap showing sentiment distribution over time
                    if not sentiment_trend.empty:
                        # Create weekly bins for better visualization
                        sentiment_trend['WEEK'] = pd.to_datetime(sentiment_trend['DATE']).dt.strftime('%Y-%m-%W')
                        
                        # Create sentiment categories
                        def categorize_sentiment(score):
                            if score < -0.6:
                                return 'Very Negative'
                            elif score < -0.2:
                                return 'Negative'
                            elif score < 0.2:
                                return 'Neutral'
                            elif score < 0.6:
                                return 'Positive'
                            else:
                                return 'Very Positive'
                        
                        sentiment_trend['SENTIMENT_CATEGORY'] = sentiment_trend['AVG_SENTIMENT'].apply(categorize_sentiment)
                        
                        # Create pivot table for heatmap
                        heatmap_data = sentiment_trend.pivot_table(
                            index='WEEK',
                            columns='SENTIMENT_CATEGORY',
                            values='AVG_SENTIMENT',
                            aggfunc='count',
                            fill_value=0
                        )
                        
                        # Sort the columns in order of sentiment
                        sentiment_order = ['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
                        heatmap_data = heatmap_data.reindex(columns=sentiment_order)
                        
                        # Get current theme
                        theme = get_current_theme()
                        
                        fig = go.Figure(data=go.Heatmap(
                            z=heatmap_data.values,
                            x=heatmap_data.columns,
                            y=heatmap_data.index,
                            colorscale='RdYlGn',
                            colorbar=dict(
                                title='Count',
                                tickfont=dict(color=theme['text'])
                            )
                        ))
                        
                        fig.update_layout(
                            xaxis_title="Sentiment Category",
                            yaxis_title="Week",
                            height=400,
                            margin=dict(l=20, r=20, t=40, b=20),
                            paper_bgcolor=theme['background'],
                            plot_bgcolor=theme['background'],
                            font=dict(color=theme['text']),
                            xaxis=dict(
                                gridcolor=theme['border'],
                                linecolor=theme['border'],
                                tickfont=dict(color=theme['text'])
                            ),
                            yaxis=dict(
                                gridcolor=theme['border'],
                                linecolor=theme['border'],
                                tickfont=dict(color=theme['text'])
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No sentiment trend data available for the selected filters.")
            else:
                st.info("No distribution data available for the selected filters.")
        
        # Create a container for download buttons
        button_container = st.container()
        with button_container:
            col1, col2 = st.columns(2)
            with col1:
                if not sentiment_dist.empty:
                    safe_download_button(
                        data=sentiment_dist.to_csv(index=False),
                        filename="sentiment_trend.csv",
                        label="⇓ Download Trend Data",
                        mime_type="text/csv",
                        help_text="Download the sentiment trend data as CSV",
                        key="download_sentiment_trend_overview"
                    )
            with col2:
                if not sentiment_dist.empty:
                    safe_download_button(
                        data=sentiment_dist.to_csv(index=False),
                        filename="sentiment_distribution.csv",
                        label="⇓ Download Distribution Data",
                        mime_type="text/csv",
                        help_text="Download the sentiment distribution data as CSV",
                        key="download_sentiment_dist_overview"
                    )
    
    # Churn Risk Section
    with st.expander("Churn Risk Analysis", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Churn Risk Analysis</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Visualizes customer churn risk across different personas. Use this chart to identify high-risk customer segments and take proactive measures to prevent churn.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        if not risk_data.empty:
            # Create color scale for risk levels with enhanced colors (outer ring only)
            risk_colors = {
                'Low': '#2ecc71',    # green
                'Medium': '#f39c12',  # orange
                'High': '#e74c3c'     # red
            }
            # Assign unique, non-green colors to each persona (middle ring)
            persona_colors = {
                'neutral': '#D3D3D3',      # Light Grey
                'satisfied': '#A9A9A9',    # Medium Light Grey
                'frustrated': '#696969',   # Medium Dark Grey
                'mixed': '#505050',       # Dark Grey
                'new': '#E8E8E8',          # Very Light Grey
                # This palette is chosen to be distinct from risk colors (green, orange, red)
                # and the total_color (blue).
            }
            total_color = '#3498db'  # Center (Total) is blue
            
            # Create hierarchical data for sunburst
            # First create a DataFrame with the correct hierarchy
            sunburst_data = []
            
            # Add total level
            total_customers = risk_data['CUSTOMER_COUNT'].sum()
            sunburst_data.append({
                'ids': 'Total',
                'labels': 'Total',
                'parents': '',
                'values': total_customers,
                'colors': total_color  # Center is blue
            })
            
            # Add persona level
            for persona in risk_data['PERSONA'].unique():
                persona_data = risk_data[risk_data['PERSONA'] == persona]
                persona_total = persona_data['CUSTOMER_COUNT'].sum()
                sunburst_data.append({
                    'ids': f'Total-{persona}',
                    'labels': persona,
                    'parents': 'Total',
                    'values': persona_total,
                    'colors': persona_colors.get(persona, '#9b59b6')  # fallback to purple if not found
                })
                
                # Add risk level for each persona
                for _, row in persona_data.iterrows():
                    sunburst_data.append({
                        'ids': f'Total-{persona}-{row["CHURN_RISK"]}',
                        'labels': row['CHURN_RISK'],
                        'parents': f'Total-{persona}',
                        'values': row['CUSTOMER_COUNT'],
                        'colors': risk_colors[row['CHURN_RISK']]
                    })
            
            # Convert to DataFrame
            sunburst_df = pd.DataFrame(sunburst_data)
            
            # Create the sunburst chart
            fig = go.Figure(go.Sunburst(
                ids=sunburst_df['ids'],
                labels=sunburst_df['labels'],
                parents=sunburst_df['parents'],
                values=sunburst_df['values'],
                marker=dict(
                    colors=sunburst_df['colors']
                ),
                branchvalues='total',
                hovertemplate="<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percentParent:.1%}<extra></extra>"
            ))
            
            # Get current theme
            theme = get_current_theme()
            
            fig.update_layout(
                title=dict(
                    text="",
                    x=0.5,
                    y=0.95,
                    xanchor='center',
                    yanchor='top',
                    font=dict(size=20, color=theme['text'])
                ),
                height=500,
                margin=dict(t=50, l=0, r=0, b=0),
                paper_bgcolor=theme['background'],
                plot_bgcolor=theme['background'],
                font=dict(color=theme['text']),
                hoverlabel=dict(
                    bgcolor=theme['background'],
                    font=dict(color=theme['text'])
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add download button for risk data
            safe_download_button(
                data=risk_data.to_csv(index=False),
                filename="churn_risk_data.csv",
                label="⇓ Download Risk Data",
                mime_type="text/csv",
                help_text="Download the churn risk data as CSV",
                key="download_risk_data_overview"
            )
        else:
            st.info("No churn risk data available for the selected filters.")
    
    # Add accessibility information
    st.markdown("""
    <div class="sr-only">
    </div>
    """, unsafe_allow_html=True)
    
    # Add mobile-specific navigation
    st.markdown("""
    <style>
    @media (max-width: 768px) {
        .stMetric {
            margin-bottom: 1rem;
        }
        .stPlotlyChart {
            margin-bottom: 1rem;
        }
        .stDataFrame {
            margin-bottom: 1rem;
        }
        .stButton>button {
            width: 100%;
        }
    }
    </style>
    """, unsafe_allow_html=True) 