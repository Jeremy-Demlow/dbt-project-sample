import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pandas as pd

from utils.database import run_query
from utils.kpi_cards import render_kpis
from utils.debug import display_debug_info, read_sql_file
from utils.theme import get_current_theme
from utils.snowflake_compatibility import safe_download_button, clean_chart_data

# Helper functions for trend calculation (copied from support_ops.py)
def calculate_delta(trend_series, is_count_metric=False):
    """Calculate the percentage change.
    
    Args:
        trend_series: Series containing the trend data
        is_count_metric: If True, treat as a count metric (like total tickets)
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
            return 0 # Avoid division by zero
            
        # Calculate percentage change
        return ((current_week - previous_week) / previous_week) * 100
    else:
        # For other metrics, compare last two values
        current = trend_series.iloc[-1]
        previous = trend_series.iloc[-2]
        
        if previous == 0:
            return 0 # Avoid division by zero
            
        return ((current - previous) / abs(previous)) * 100

def get_smoothed_trend_data(df, column_name, window=30):
    """Get smoothed trend data using a moving average.
    
    Args:
        df: DataFrame containing the trend data, must have a 'date' column or index.
        column_name: Name of the column to smooth.
        window: Window size for moving average (default: 30 days).
        
    Returns:
        pd.Series: Smoothed trend data, or None if input is invalid.
    """
    if df is None or column_name not in df.columns:
        return None
    
    # Ensure 'date' column exists and is set as index
    if 'date' not in df.columns and df.index.name != 'date':
        st.error(f"DataFrame for '{column_name}' must have a 'date' column or index for smoothing.")
        return None
        
    data_series = df.set_index('date')[column_name] if 'date' in df.columns else df[column_name]
    
    if data_series.empty:
        return pd.Series(dtype=float) # Return empty series if no data

    return data_series.rolling(window=window, min_periods=1).mean()

# Load data with caching
@st.cache_data(ttl=300)
def load_rating_trend():
    query = "product_feedback/rating_trend.sql"
    results = run_query(query)
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    return df

@st.cache_data(ttl=300)
def load_rating_distribution():
    query = "product_feedback/rating_distribution.sql"
    results = run_query(query)
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    return df

@st.cache_data(ttl=300)
def load_sentiment_by_language():
    query = "product_feedback/sentiment_by_language.sql"
    results = run_query(query)
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    return df

@st.cache_data(ttl=300)
def load_recent_reviews():
    query = "product_feedback/recent_reviews.sql"
    results = run_query(query)
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    return df

def render_product_feedback(filters, debug_mode=False):
    """
    Renders the Product Feedback dashboard tab.
    
    This component displays:
    1. Rating trends over time
    2. Rating distribution
    3. Sentiment analysis by language
    4. Recent reviews with sentiment analysis
    
    Args:
        filters: Dictionary containing filter values
        debug_mode: Whether to show debug information
    """
    # st.title("Product Feedback Analysis") # Old title
    st.markdown("""
    <style>
    h2 {
        padding: 0 !important;
        margin: 0 !important;
    }
    </style>
    <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem;">
        <h2>Product Feedback Analysis</h2>
        <div class="tooltip">
            <span class="help-icon">?</span>
            <span class="tooltiptext">
                Analyze product feedback including rating trends, sentiment by language, and recent reviews.
                <br><br>
                Use the filters to refine the data and explore insights.
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load all data first
    with st.spinner("Loading product feedback data..."):
        rating_trend_data = load_rating_trend()
        rating_dist_data = load_rating_distribution()
        sentiment_lang_data = load_sentiment_by_language()
        recent_reviews_data = load_recent_reviews()
    
    # Display debug information for filters and all queries if debug mode is enabled
    if st.session_state.get('debug_mode', False):
        st.markdown("### Debug Information")
        
        # Show current filters (still displayed, though not used by this tab's queries anymore)
        st.markdown("#### Current Filters (Not actively used by Product Feedback queries)")
        st.json({
            "start_date": filters["start_date"],
            "end_date": filters["end_date"],
            "personas": filters.get("personas", [])
        })
        
        # Show debug info for each query
        queries = [
            ("Rating Trend Query", "product_feedback/rating_trend.sql", rating_trend_data),
            ("Rating Distribution Query", "product_feedback/rating_distribution.sql", rating_dist_data),
            ("Sentiment by Language Query", "product_feedback/sentiment_by_language.sql", sentiment_lang_data),
            ("Recent Reviews Query", "product_feedback/recent_reviews.sql", recent_reviews_data)
        ]
        
        for query_name, sql_file, results in queries:
            # Params are no longer used by these queries for this tab
            display_debug_info(
                sql_file_path=sql_file,
                params={}, # No specific params for these queries now
                results=results,
                query_name=query_name
            )
    
    # KPI Row
    avg_rating = rating_trend_data['avg_rating'].mean()
    total_reviews = rating_trend_data['review_count'].sum()
    avg_sentiment = sentiment_lang_data['avg_sentiment'].mean()

    # Prepare trend data for KPIs
    # Ensure rating_trend_data has a 'date' column for smoothing
    if 'date' not in rating_trend_data.columns:
        # Attempt to use index if it's date-like, otherwise create a dummy date range if necessary
        # This part might need adjustment based on actual data structure if 'date' column is missing
        if isinstance(rating_trend_data.index, pd.DatetimeIndex):
            rating_trend_data_for_smoothing = rating_trend_data.reset_index().rename(columns={'index':'date'})
        else: # Fallback, consider logging a warning or specific handling
            st.warning("Rating trend data is missing a 'date' column for smoothing. Sparklines might not be accurate for some KPIs.")
            rating_trend_data_for_smoothing = rating_trend_data.assign(date=pd.date_range(end=datetime.now(), periods=len(rating_trend_data)))
    else:
        rating_trend_data_for_smoothing = rating_trend_data

    avg_rating_trend_smoothed = get_smoothed_trend_data(rating_trend_data_for_smoothing, 'avg_rating')
    avg_rating_delta = calculate_delta(avg_rating_trend_smoothed)

    total_reviews_trend_smoothed = get_smoothed_trend_data(rating_trend_data_for_smoothing, 'review_count')
    total_reviews_delta = calculate_delta(total_reviews_trend_smoothed, is_count_metric=True)

    # Prepare sentiment trend data
    if not sentiment_lang_data.empty and 'review_date' in sentiment_lang_data.columns:
        sentiment_trend_raw = sentiment_lang_data.groupby('review_date')['avg_sentiment'].mean().reset_index()
        sentiment_trend_raw.rename(columns={'review_date': 'date'}, inplace=True)
        avg_sentiment_trend_smoothed = get_smoothed_trend_data(sentiment_trend_raw, 'avg_sentiment')
        avg_sentiment_delta = calculate_delta(avg_sentiment_trend_smoothed)
    else:
        avg_sentiment_trend_smoothed = pd.Series(dtype=float)
        avg_sentiment_delta = 0

    # Prepare 5-Star Review Count KPI
    five_star_reviews_count = 0
    five_star_trend_for_sparkline = pd.Series(dtype=float) # Will be 7-day moving SUM
    five_star_delta = 0

    if not rating_dist_data.empty and 'review_rating' in rating_dist_data.columns and 'count' in rating_dist_data.columns:
        five_star_data = rating_dist_data[rating_dist_data['review_rating'] == 5]
        if not five_star_data.empty:
            five_star_reviews_count = five_star_data['count'].sum()

    if not recent_reviews_data.empty and 'review_date' in recent_reviews_data.columns and 'review_rating' in recent_reviews_data.columns:
        # Ensure 'review_date' is datetime for original DataFrame before grouping
        recent_reviews_data['review_date'] = pd.to_datetime(recent_reviews_data['review_date'])
        
        # Create daily counts of 5-star reviews
        five_star_daily_df = recent_reviews_data[recent_reviews_data['review_rating'] == 5].groupby(
            pd.Grouper(key='review_date', freq='D')
        ).size().reset_index(name='count')
        five_star_daily_df.rename(columns={'review_date': 'date'}, inplace=True)

        if not five_star_daily_df.empty:
            # Ensure 'date' column is datetime after manipulation, if not already
            five_star_daily_df['date'] = pd.to_datetime(five_star_daily_df['date'])
            daily_counts_series = five_star_daily_df.set_index('date')['count']
            
            # For sparkline: 7-day moving sum of daily 5-star reviews
            five_star_trend_for_sparkline = daily_counts_series.rolling(window=7, min_periods=1).sum()
            
            # For delta: week-over-week change of actual daily 5-star review counts
            # calculate_delta expects a Series with a DatetimeIndex if it's time-based, 
            # or it operates on the implicit index if not.
            # Here, daily_counts_series has a DatetimeIndex.
            five_star_delta = calculate_delta(daily_counts_series, is_count_metric=True)
        else:
            five_star_trend_for_sparkline = pd.Series(dtype=float) # Ensure it's an empty Series
            five_star_delta = 0
    
    render_kpis([
        {
            'label': "Average Rating",
            'value': f"{avg_rating:.1f}",
            'delta': avg_rating_delta,
            'help': "Average product rating across all reviews. Trend shows 7-day moving average.",
            'trend_data': avg_rating_trend_smoothed
        },
        {
            'label': "Total Reviews",
            'value': f"{total_reviews:,}",
            'delta': total_reviews_delta,
            'help': "Total number of product reviews. Trend shows 7-day moving sum, delta is week-over-week change.",
            'trend_data': total_reviews_trend_smoothed
        },
        {
            'label': "Average Sentiment",
            'value': f"{avg_sentiment:.2f}",
            'delta': avg_sentiment_delta,
            'help': "Average sentiment score across all reviews. Trend shows 7-day moving average.",
            'trend_data': avg_sentiment_trend_smoothed
        },
        {
            'label': "5-Star Reviews",
            'value': f"{five_star_reviews_count:,}",
            'delta': five_star_delta,
            'help': "Total count of 5-star reviews. Trend shows 7-day moving sum of daily 5-star reviews.",
            'trend_data': five_star_trend_for_sparkline # Use the new rolling sum series
        }
    ], columns=4)
    
    # Rating Trend Chart
    with st.expander("Rating Trend Analysis", expanded=True):
        st.subheader("Rating Trend Over Time")
        if not rating_trend_data.empty:
            # Get current theme
            theme = get_current_theme()

            # Ensure 'date' column is datetime for plotting
            rating_trend_data['date'] = pd.to_datetime(rating_trend_data['date'])
            
            # Prepare data for smoothed trend line
            # avg_rating_trend_smoothed is already a Series with a DatetimeIndex
            # We need to align its x-values with rating_trend_data['date'] if it's not already aligned
            # For simplicity, we assume avg_rating_trend_smoothed index matches rating_trend_data['date']
            # or we can reindex/merge if necessary. The existing code for get_smoothed_trend_data
            # should produce a series that can be plotted against a date axis.

            # Create figure with secondary y-axis
            from plotly.subplots import make_subplots
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # 1. Smoothed Average Rating Line
            # Ensure avg_rating_trend_smoothed has a compatible index for plotting
            # If avg_rating_trend_smoothed is a Series with DatetimeIndex:
            if avg_rating_trend_smoothed is not None and not avg_rating_trend_smoothed.empty:
                fig.add_trace(
                    go.Scatter(
                        x=avg_rating_trend_smoothed.index, # Assumes DatetimeIndex
                        y=avg_rating_trend_smoothed.values,
                        name='7-Day Smoothed Avg Rating',
                        mode='lines',
                        line=dict(color=theme.get('secondary', '#ff7f0e')), # Use theme color or default for smoothed line
                        hovertemplate='<b>Date</b>: %{x|%Y-%m-%d}<br>' +
                                      '<b>Smoothed Avg Rating</b>: %{y:.2f}<extra></extra>'
                    ),
                    secondary_y=False,
                )

            # 2. Review Volume Bars
            fig.add_trace(
                go.Bar(
                    x=rating_trend_data['date'],
                    y=rating_trend_data['review_count'],
                    name='Review Volume',
                    marker=dict(color=theme.get('tertiary', '#2ca02c'), opacity=0.6), # Use theme color or default
                    hovertemplate='<b>Date</b>: %{x|%Y-%m-%d}<br>' +
                                  '<b>Review Count</b>: %{y}<extra></extra>'
                ),
                secondary_y=True,
            )
            
            # 5. Layout Updates (includes X-axis range slider by default)
            fig.update_layout(
                title_text='Average Rating and Review Volume Over Time',
                paper_bgcolor=theme['background'],
                plot_bgcolor=theme['background'],
                font=dict(color=theme['text']),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified", # Improved hover experience
                xaxis=dict(
                    title='Date',
                    gridcolor=theme['border'],
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text']),
                    rangeslider_visible=True # Explicitly enable range slider
                ),
                yaxis=dict(
                    title='Average Rating (1-5)',
                    range=[1, 5.1], # Optimize Y-Axis Scale
                    dtick=0.5, # Sensible tick marks
                    gridcolor=theme['border'],
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text']),
                    showgrid=True 
                ),
                yaxis2=dict(
                    title='Review Volume',
                    overlaying='y',
                    side='right',
                    gridcolor=theme.get('light_border', 'rgba(204,204,204,0.2)'), # Lighter grid for secondary axis
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text']),
                    showgrid=False # Optionally hide secondary grid or make it lighter
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No rating data available for the selected period.")
    
    # Sentiment by Language Chart
    with st.expander("Sentiment by Language", expanded=True):
        st.subheader("Sentiment Analysis by Language")
        if not sentiment_lang_data.empty:
            pivot_df = sentiment_lang_data.pivot(
                index='review_language',
                columns='review_date',
                values='avg_sentiment'
            )
            
            # Get current theme
            theme = get_current_theme()
            
            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns,
                y=pivot_df.index,
                colorscale='RdBu',
                zmid=0,
                hoverongaps=False
            ))
            
            fig.update_layout(
                title='Average Sentiment by Language Over Time',
                xaxis_title='Date',
                yaxis_title='Language',
                height=400,
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
            st.info("No sentiment data available by language.")
    
    # Recent Reviews Section
    with st.expander("Recent Reviews", expanded=True):
        st.subheader("Recent Reviews (Translated)")
        col1, col2, col3 = st.columns(3)
    
    with col1:
        rating_range = st.slider("Rating Range", 1, 5, (1, 5))
    
    with col2:
        sentiment_filter = st.selectbox("Sentiment Filter", 
                                      ["All", "Positive", "Neutral", "Negative"])
    
    with col3:
        available_languages = ["All"]
        if not recent_reviews_data.empty and 'review_language' in recent_reviews_data.columns:
            available_languages.extend(sorted(recent_reviews_data['review_language'].unique().tolist()))
        selected_language = st.selectbox("Filter by Language", available_languages, key="language_filter")

    # Filter reviews
    filtered_reviews = recent_reviews_data.copy()
    min_selected_rating, max_selected_rating = rating_range
    filtered_reviews = filtered_reviews[filtered_reviews['review_rating'].between(min_selected_rating, max_selected_rating)]
    
    if selected_language != "All":
        filtered_reviews = filtered_reviews[filtered_reviews['review_language'] == selected_language]

    if sentiment_filter != "All":
        if sentiment_filter == "Positive":
            filtered_reviews = filtered_reviews[filtered_reviews['sentiment_score'] > 0.2]
        elif sentiment_filter == "Neutral":
            filtered_reviews = filtered_reviews['sentiment_score'].between(-0.2, 0.2)
        else:
            filtered_reviews = filtered_reviews[filtered_reviews['sentiment_score'] < -0.2]
    
    # Display reviews
    for _, review in filtered_reviews.iterrows():
        with st.expander(f"Review {review['review_id']} - Rating: {review['review_rating']} ⭐"):
            st.write(f"**Date:** {review['review_date'].strftime('%Y-%m-%d')}")
            st.write(f"**Language:** {review['review_language']}")
            st.write(f"**Sentiment Score:** {review['sentiment_score']:.2f}")
            st.write("**Original Text:**")
            st.write(review['review_text'])
            if review['review_language'] != 'en':
                st.write("**English Translation:**")
                st.write(review['review_text_english'])
                
    # Add download buttons for data
    with st.expander("Download Datasets", expanded=True):
        st.subheader("Download Data")
        col1, col2 = st.columns(2)
        with col1:
            safe_download_button(
                data=rating_trend_data.to_csv(index=False),
                filename="rating_trend.csv",
                label="⇓ Download Rating Trend Data",
                mime_type="text/csv",
                help_text="Download the rating trend data as CSV",
                key="download_rating_trend_product"
            )
            safe_download_button(
                data=rating_dist_data.to_csv(index=False),
                filename="rating_distribution.csv",
                label="⇓ Download Rating Distribution Data",
                mime_type="text/csv",
                help_text="Download the rating distribution data as CSV",
                key="download_rating_distribution_product"
            )
        with col2:
            safe_download_button(
                data=sentiment_lang_data.to_csv(index=False),
                filename="sentiment_by_language.csv",
                label="⇓ Download Language Sentiment Data",
                mime_type="text/csv",
                help_text="Download the sentiment by language data as CSV",
                key="download_language_sentiment_product"
            )
            safe_download_button(
                data=recent_reviews_data.to_csv(index=False),
                filename="recent_reviews.csv",
                label="⇓ Download Recent Reviews Data",
                mime_type="text/csv",
                help_text="Download the recent reviews data as CSV",
                key="download_recent_reviews_product"
            ) 