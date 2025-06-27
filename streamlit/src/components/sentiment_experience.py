"""
Sentiment & Experience dashboard component showing sentiment trends and analysis.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.database import snowflake_conn
from utils.kpi_cards import render_kpis
from utils.debug import display_debug_info, read_sql_file
from utils.theme import get_current_theme
from utils.snowflake_compatibility import safe_download_button, clean_chart_data
from typing import Dict, Any
import numpy as np
import json
from decimal import Decimal
from scipy.stats import gaussian_kde

def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

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
        trend_series = df.set_index('date')[column_name]
        return trend_series.rolling(window=window, min_periods=1).mean()
    return None

def render_sentiment_experience(filters: Dict[str, Any], debug_mode: bool = False) -> None:
    """Render the Sentiment & Experience dashboard.
    
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
        <h2>Sentiment & Experience Analysis</h2>
        <div class="tooltip">
            <span class="help-icon">?</span>
            <span class="tooltiptext">
                Analyze customer sentiment trends, channel alignment, and experience metrics across different sources and personas.
                <br><br>
                Press Tab to navigate through the dashboard sections. Use Enter to expand/collapse sections.
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load all data first
    with st.spinner("Loading sentiment data..."):
        sentiment_time_query = "sentiment_experience/sentiment_over_time.sql"
        sentiment_time_df = pd.DataFrame(snowflake_conn.execute_query(sentiment_time_query))
        
        sentiment_dist_query = "sentiment_experience/sentiment_distribution.sql"
        sentiment_dist_df = pd.DataFrame(snowflake_conn.execute_query(sentiment_dist_query))
        
        sentiment_by_persona_query = "sentiment_experience/sentiment_by_persona.sql"
        sentiment_by_persona_df = pd.DataFrame(snowflake_conn.execute_query(sentiment_by_persona_query))
        
        volatility_trend_query = "sentiment_experience/volatility_vs_trend.sql"
        volatility_trend_df = pd.DataFrame(snowflake_conn.execute_query(volatility_trend_query))
        
        channel_alignment_query = "sentiment_experience/channel_alignment.sql"
        channel_alignment_df = pd.DataFrame(snowflake_conn.execute_query(channel_alignment_query))
        
        sentiment_recovery_query = "sentiment_experience/sentiment_recovery_rate.sql"
        sentiment_recovery_df = pd.DataFrame(snowflake_conn.execute_query(sentiment_recovery_query))
    
    # Display debug information for filters and all queries if debug mode is enabled
    if st.session_state.get('debug_mode', False):
        st.markdown("### Debug Information")
        
        # Show debug info for each query
        queries = [
            ("Sentiment Over Time Query", sentiment_time_query, {}, sentiment_time_df),
            ("Sentiment Distribution Query", sentiment_dist_query, {}, sentiment_dist_df),
            ("Sentiment by Persona Query", sentiment_by_persona_query, {}, sentiment_by_persona_df),
            ("Volatility vs Trend Query", volatility_trend_query, {}, volatility_trend_df),
            ("Channel Alignment Query", channel_alignment_query, {}, channel_alignment_df),
            ("Sentiment Recovery Query", sentiment_recovery_query, {}, sentiment_recovery_df)
        ]
        
        for query_name, sql_file, params, results in queries:
            display_debug_info(
                sql_file_path=sql_file,
                params=params,
                results=results,
                query_name=query_name
            )
    
    if sentiment_time_df.empty:
        st.warning("No sentiment data available.")
        return
    
    # Convert column names to lowercase
    sentiment_time_df.columns = sentiment_time_df.columns.str.lower()
    sentiment_dist_df.columns = sentiment_dist_df.columns.str.lower()
    sentiment_by_persona_df.columns = sentiment_by_persona_df.columns.str.lower()
    volatility_trend_df.columns = volatility_trend_df.columns.str.lower()
    channel_alignment_df.columns = channel_alignment_df.columns.str.lower()
    sentiment_recovery_df.columns = sentiment_recovery_df.columns.str.lower()
    
    if debug_mode:
        st.write("Sentiment Time DF Columns:", sentiment_time_df.columns.tolist())
        st.write("Sentiment Time DF Sample:", sentiment_time_df.head())
    
    # Key Metrics Section
    try:
        # 1. Sentiment Consistency Score (standard deviation of sentiment scores)
        sentiment_consistency = sentiment_by_persona_df['sentiment_volatility'].mean()
        sentiment_consistency_trend = get_smoothed_trend_data(
            sentiment_by_persona_df.groupby('date')['sentiment_volatility'].mean().reset_index(),
            'sentiment_volatility'
        )
        
        # 2. Cross-channel Sentiment Alignment (correlation between different channel sentiments)
        channel_pivot = channel_alignment_df.pivot(index='date', columns='source_type', values='avg_sentiment')
        channel_correlation = channel_pivot.corr().mean().mean() if channel_pivot.shape[1] > 1 else 0.0
        channel_alignment_trend = get_smoothed_trend_data(
            channel_alignment_df.groupby('date')['avg_sentiment'].std().reset_index(),
            'avg_sentiment'
        )
        
        # 3. Customer Experience Score (weighted average of sentiment, rating, and support metrics)
        daily_experience_score = (
            sentiment_time_df.groupby('date')['avg_sentiment'].mean() * 0.4 +
            (1 - sentiment_by_persona_df.groupby('date')['sentiment_volatility'].mean()) * 0.3 +
            channel_alignment_df.groupby('date')['avg_sentiment'].std() * 0.3
        )
        experience_score_trend = daily_experience_score.rolling(window=7, min_periods=1).mean()
        experience_score = daily_experience_score.mean()
        
        # 4. Sentiment Recovery Rate (% of negative sentiments followed by positive ones)
        def rolling_recovery_rate(sentiments, window=2):
            rates = []
            for i in range(len(sentiments)):
                start = max(0, i - window + 1)
                window_sents = sentiments[start:i+1]
                neg_to_pos = sum(1 for j in range(len(window_sents)-1) if window_sents[j] < 0 and window_sents[j+1] > 0)
                total_neg = sum(1 for j in range(len(window_sents)-1) if window_sents[j] < 0)
                rate = (neg_to_pos / total_neg * 100) if total_neg > 0 else 0.0  # Return 0.0 instead of np.nan
                rates.append(rate)
            return rates
        sentiment_recovery_df_sorted = sentiment_recovery_df.sort_values('date')
        sentiments = sentiment_recovery_df_sorted['avg_sentiment'].values
        daily_recovery_rates = pd.Series(rolling_recovery_rate(sentiments, window=2), index=sentiment_recovery_df_sorted['date'])
        recovery_trend = daily_recovery_rates.rolling(window=7, min_periods=1).mean()  # Apply 7-day smoothing
        recovery_rate = np.nanmean(recovery_trend) if not np.isnan(recovery_trend).all() else 0.0  # Handle all-nan case
        
        # Calculate deltas for each KPI using trend data
        def calculate_delta(trend_data):
            if trend_data is None or len(trend_data) < 7:
                return 0.0
            current = trend_data.iloc[-1]
            previous = trend_data.iloc[-7]
            # Handle nan values
            if pd.isna(current) or pd.isna(previous) or previous == 0:
                return 0.0
            return ((current - previous) / abs(previous)) * 100

        kpi_data = [
            {
                "label": "Sentiment Consistency",
                "value": f"{sentiment_consistency:.2f}",
                "help": "Standard deviation of sentiment scores across all interactions",
                "trend_data": sentiment_consistency_trend,
                "delta": calculate_delta(sentiment_consistency_trend)
            },
            {
                "label": "Channel Alignment",
                "value": f"{channel_correlation:.2f}",
                "help": "Correlation between sentiment scores across different channels",
                "trend_data": channel_alignment_trend,
                "delta": calculate_delta(channel_alignment_trend)
            },
            {
                "label": "Experience Score",
                "value": f"{experience_score:.2f}",
                "help": "Weighted average of sentiment, support metrics, and channel alignment",
                "trend_data": experience_score_trend,
                "delta": calculate_delta(experience_score_trend)
            },
            {
                "label": "Recovery Rate",
                "value": f"{recovery_rate:.1f}%",
                "help": "Percentage of negative sentiments followed by positive ones",
                "trend_data": recovery_trend,
                "delta": calculate_delta(recovery_trend)
            }
        ]
        
        render_kpis(kpi_data, columns=4)
        
        # Add download button for KPI data
        kpi_data_for_json = [
            {
                "label": kpi["label"],
                "value": kpi["value"],
                "help": kpi["help"],
                "delta": kpi["delta"],
                "trend_data": kpi["trend_data"].tolist() if isinstance(kpi["trend_data"], pd.Series) else None
            }
            for kpi in kpi_data
        ]
        
        safe_download_button(
            data=json.dumps(kpi_data_for_json, indent=2, default=decimal_to_float),
            filename="sentiment_kpi_data.json",
            label="⇓ Download KPI Data",
            mime_type="application/json",
            help_text="Download the current KPI data as JSON",
            key="download_kpi_data_sentiment"
        )
        
    except KeyError as e:
        st.error(f"Error calculating KPIs: Missing column {str(e)}")
        if debug_mode:
            st.write("Available columns:", sentiment_time_df.columns.tolist())
        return
    
    # Sentiment Over Time Section
    with st.expander("Sentiment Over Time", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Sentiment Over Time by Source</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Track sentiment trends across different channels and sources over time. Use this to identify patterns and correlations in customer feedback.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        if not sentiment_time_df.empty:
            # Get current theme
            theme = get_current_theme()
            
            fig = px.line(
                sentiment_time_df,
                x='date',
                y='rolling_30d_avg',
                color='source_type',
                labels={
                    'date': 'Date',
                    'rolling_30d_avg': 'Average Sentiment',
                    'source_type': 'Source Type'
                }
            )
            
            fig.update_layout(
                paper_bgcolor=theme['background'],
                plot_bgcolor=theme['background'],
                font=dict(color=theme['text']),
                margin=dict(t=0, l=0, r=0, b=0),
                height=400,
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
            
            # Add download button for trend data
            safe_download_button(
                data=sentiment_time_df.to_csv(index=False),
                filename="sentiment_trend.csv",
                label="⇓ Download Trend Data",
                mime_type="text/csv",
                help_text="Download the sentiment trend data as CSV",
                key="download_sentiment_trend_experience"
            )
        else:
            st.info("No sentiment trend data available.")
    
    # Sentiment Distribution Section
    with st.expander("Sentiment Distribution", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Sentiment Distribution by Source</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    View the distribution of sentiment scores across different channels and sources. The overlapping density plots show how sentiment is distributed for each source, making it easy to compare patterns and identify differences.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        if not sentiment_dist_df.empty:
            # Get current theme
            theme = get_current_theme()
            
            # Create figure
            fig = go.Figure()
            
            # Get unique source types
            source_types = sentiment_dist_df['source_type'].unique()
            
            # Create a color palette that's intuitive for sentiment
            colors = ['#d62728', '#ff7f0e', '#bcbd22', '#2ca02c', '#1f77b4']
            
            # Calculate common x range for all sources
            x_min = sentiment_dist_df['sentiment_score'].min()
            x_max = sentiment_dist_df['sentiment_score'].max()
            x_range = np.linspace(x_min, x_max, 100)
            
            # Add a density plot for each source type
            valid_sources = []
            for idx, source in enumerate(source_types):
                source_data = sentiment_dist_df[sentiment_dist_df['source_type'] == source]
                
                # Skip if we don't have enough data points
                if len(source_data) < 2:
                    continue
                    
                valid_sources.append(source)
                
                try:
                    # Calculate kernel density estimate
                    kde = gaussian_kde(source_data['sentiment_score'], weights=source_data['count'])
                    y_range = kde(x_range)
                    
                    # Normalize the density for better visualization
                    max_density = y_range.max()
                    if max_density > 0:  # Only normalize if we have non-zero values
                        y_range = y_range / max_density
                    
                    # Add the density plot with increased spacing
                    fig.add_trace(go.Scatter(
                        x=x_range,
                        y=y_range + (len(valid_sources) - 1) * 1.2,  # Use valid_sources count for spacing
                        fill='tonexty',
                        name=source,
                        line=dict(width=1),
                        fillcolor=colors[idx % len(colors)],
                        opacity=0.7,
                        showlegend=True,
                        hovertemplate="<b>Source:</b> %{fullData.name}<br>" +
                                    "<b>Sentiment Score:</b> %{x:.2f}<br>" +
                                    "<b>Density:</b> %{y:.2f}<extra></extra>"
                    ))
                except Exception as e:
                    if debug_mode:
                        st.warning(f"Error calculating density for source {source}: {str(e)}")
                    continue
            
            if not valid_sources:
                st.info("No valid sentiment distribution data available for visualization.")
                return
                
            # Add a reference line at 0
            fig.add_shape(
                type="line",
                x0=0,
                x1=0,
                y0=-0.2,
                y1=len(valid_sources) * 1.2,
                line=dict(
                    color=theme['text'],
                    width=1,
                    dash="dash",
                ),
                opacity=0.5
            )
            
            # Update layout
            fig.update_layout(
                title='Sentiment Distribution by Source',
                xaxis_title='Sentiment Score',
                yaxis_title='Source',
                height=400,
                paper_bgcolor=theme['background'],
                plot_bgcolor=theme['background'],
                font=dict(color=theme['text']),
                margin=dict(t=40, l=0, r=0, b=0),
                xaxis=dict(
                    gridcolor=theme['border'],
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text']),
                    range=[x_min, x_max]  # Set fixed x-axis range
                ),
                yaxis=dict(
                    gridcolor=theme['border'],
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text']),
                    showticklabels=False,  # Hide y-axis labels since we're using the legend
                    range=[-0.2, len(valid_sources) * 1.2]  # Use valid_sources count for range
                ),
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=1.05,
                    bgcolor=theme['background'],
                    bordercolor=theme['border'],
                    borderwidth=1
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add download button for distribution data
            safe_download_button(
                data=sentiment_dist_df.to_csv(index=False),
                filename="sentiment_distribution.csv",
                label="⇓ Download Distribution Data",
                mime_type="text/csv",
                help_text="Download the sentiment distribution data as CSV",
                key="download_sentiment_distribution_experience"
            )
        else:
            st.info("No sentiment distribution data available.")
    
    # Volatility Analysis Section
    with st.expander("Volatility Analysis", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Volatility vs Trend Analysis</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Analyze the relationship between sentiment volatility and trends. Use this to identify stable vs. volatile customer segments and their impact on overall sentiment.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        if not sentiment_by_persona_df.empty:
            theme = get_current_theme()
            
            required_cols = ['sentiment_volatility', 'sentiment_trend', 'persona', 'avg_sentiment']
            missing_cols = [col for col in required_cols if col not in sentiment_by_persona_df.columns]

            if not missing_cols:
                df_to_plot = sentiment_by_persona_df.copy()

                s_min = df_to_plot['avg_sentiment'].min()
                s_max = df_to_plot['avg_sentiment'].max()

                size_min_display = 5  # Min marker size in pixels
                size_max_display = 30 # Max marker size in pixels

                if pd.isna(s_min) or pd.isna(s_max) or s_max == s_min:
                    df_to_plot['marker_size'] = (size_min_display + size_max_display) / 2
                else:
                    normalized_sentiment = (df_to_plot['avg_sentiment'] - s_min) / (s_max - s_min)
                    df_to_plot['marker_size'] = size_min_display + normalized_sentiment * (size_max_display - size_min_display)
                
                df_to_plot['marker_size'] = df_to_plot['marker_size'].fillna(size_min_display)

                fig = px.scatter(
                    df_to_plot,
                    x='sentiment_volatility',
                    y='sentiment_trend',
                    color='persona',
                    size='marker_size', 
                    title='Sentiment Volatility vs Trend by Persona',
                    labels={
                        'sentiment_volatility': 'Sentiment Volatility',
                        'sentiment_trend': 'Sentiment Trend',
                        'persona': 'Persona',
                        'marker_size': 'Average Sentiment' 
                    },
                    hover_name='persona',
                    hover_data={
                        'avg_sentiment': ':.2f',
                        'sentiment_volatility': ':.2f',
                        'sentiment_trend': ':.2f',
                        'marker_size': False 
                    }
                )
            
                fig.update_layout(
                    paper_bgcolor=theme['background'],
                    plot_bgcolor=theme['background'],
                    font=dict(color=theme['text']),
                    margin=dict(t=40, l=0, r=0, b=0), # Adjusted top margin for title
                    height=400,
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
            
            else: # Missing required columns
                st.info(f"Cannot generate Volatility vs Trend plot: Missing required column(s): {', '.join(missing_cols)} in the data from 'sentiment_by_persona.sql'.")

            # Download button is available if dataframe is not empty, regardless of plot success
            safe_download_button(
                data=sentiment_by_persona_df.to_csv(index=False),
                filename="sentiment_volatility.csv",
                label="⇓ Download Volatility Data",
                mime_type="text/csv",
                help_text="Download the sentiment by persona data (used for volatility analysis) as CSV",
                key="download_sentiment_volatility_experience"
            )
        else:
            st.info("No volatility vs trend data available.")
    
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