"""
Support Operations dashboard component showing ticket metrics and trends.
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
from utils.snowflake_compatibility import safe_download_button, clean_chart_data
import os
from datetime import datetime, timedelta
import json
from decimal import Decimal
import numpy as np

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
def load_ticket_volume_data(filters: dict) -> pd.DataFrame:
    """Load and cache ticket volume data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Ticket volume data
    """
    query = "support_ops/ticket_volume_trend.sql"
    results = snowflake_conn.execute_query(query, 
                      {"start_date": filters["start_date"], 
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Ticket Volume Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_priority_data(filters: dict) -> pd.DataFrame:
    """Load and cache priority data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Priority data
    """
    query = "support_ops/priority_breakdown.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Priority Breakdown Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_category_data(filters: dict) -> pd.DataFrame:
    """Load and cache category data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Category data
    """
    query = "support_ops/category_analysis.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Category Analysis Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_tickets_per_customer_data(filters: dict) -> pd.DataFrame:
    """Load and cache tickets per customer data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Tickets per customer data
    """
    query = "support_ops/tickets_per_customer.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Tickets per Customer Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_first_response_data(filters: dict) -> pd.DataFrame:
    """Load and cache first response time data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: First response time data
    """
    query = "support_ops/first_response_time.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="First Response Time Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_resolution_rate_data(filters: dict) -> pd.DataFrame:
    """Load and cache resolution rate data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Resolution rate data
    """
    query = "support_ops/resolution_rate.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Resolution Rate Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_customer_effort_data(filters: dict) -> pd.DataFrame:
    """Load and cache customer effort data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Customer effort data
    """
    query = "support_ops/customer_effort.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Customer Effort Query"
        )
    
    return df

@st.cache_data(ttl=300)
def load_channel_effectiveness_data(filters: dict) -> pd.DataFrame:
    """Load and cache channel effectiveness data.
    
    Args:
        filters: Dictionary containing date range and persona filters
        
    Returns:
        pd.DataFrame: Channel effectiveness data
    """
    query = "support_ops/channel_effectiveness.sql"
    results = snowflake_conn.execute_query(query,
                      {"start_date": filters["start_date"],
                       "end_date": filters["end_date"]})
    df = pd.DataFrame(results)
    df.columns = df.columns.str.lower()
    
    if st.session_state.get('debug_mode', False):
        display_debug_info(
            sql_file_path=query,
            params={"start_date": filters["start_date"], "end_date": filters["end_date"]},
            results=df,
            query_name="Channel Effectiveness Query"
        )
    
    return df

def calculate_delta(trend_series, is_count_metric=False):
    """Calculate the percentage change between the last two values.
    
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
        
        if pd.isna(current_week) or pd.isna(previous_week) or previous_week == 0:
            return 0
            
        # Calculate percentage change
        return ((current_week - previous_week) / previous_week) * 100
    else:
        # For other metrics, compare last two values
        current = trend_series.iloc[-1]
        previous = trend_series.iloc[-2]
        
        if pd.isna(current) or pd.isna(previous) or previous == 0:
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
        trend_series = df.set_index('date')[column_name]
        return trend_series.rolling(window=window, min_periods=1).mean()
    return None

def render_support_ops_dashboard(filters: dict, debug_mode: bool = False) -> None:
    """Render the Support Operations dashboard.
    
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
        <h2>Support Operations Dashboard</h2>
        <div class="tooltip">
            <span class="help-icon">?</span>
            <span class="tooltiptext">
                Track support ticket metrics, trends, and patterns across different categories and priorities.
                <br><br>
                Press Tab to navigate through the dashboard sections. Use Enter to expand/collapse sections.
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load all data first
    with st.spinner("Loading support operations data..."):
        ticket_volume_data = load_ticket_volume_data(filters)
        priority_data = load_priority_data(filters)
        first_response_data = load_first_response_data(filters)
        resolution_rate_data = load_resolution_rate_data(filters)
        customer_effort_data = load_customer_effort_data(filters)
        channel_effectiveness_data = load_channel_effectiveness_data(filters)
    
    # Display debug information for filters and all queries if debug mode is enabled
    if st.session_state.get('debug_mode', False):
        st.markdown("### Debug Information")
        
        # Show current filters
        st.markdown("#### Current Filters")
        st.json({
            "start_date": filters["start_date"],
            "end_date": filters["end_date"],
            "personas": filters.get("personas", [])
        })
        
        # Show debug info for each query
        queries = [
            ("Ticket Volume Trend Query", "support_ops/ticket_volume_trend.sql", {
                "start_date": filters["start_date"],
                "end_date": filters["end_date"]
            }, ticket_volume_data),
            ("Priority Breakdown Query", "support_ops/priority_breakdown.sql", {
                "start_date": filters["start_date"],
                "end_date": filters["end_date"]
            }, priority_data),
            ("First Response Time Query", "support_ops/first_response_time.sql", {
                "start_date": filters["start_date"],
                "end_date": filters["end_date"]
            }, first_response_data),
            ("Resolution Rate Query", "support_ops/resolution_rate.sql", {
                "start_date": filters["start_date"],
                "end_date": filters["end_date"]
            }, resolution_rate_data),
            ("Customer Effort Query", "support_ops/customer_effort.sql", {
                "start_date": filters["start_date"],
                "end_date": filters["end_date"]
            }, customer_effort_data),
            ("Channel Effectiveness Query", "support_ops/channel_effectiveness.sql", {
                "start_date": filters["start_date"],
                "end_date": filters["end_date"]
            }, channel_effectiveness_data)
        ]
        
        for query_name, sql_file, params, results in queries:
            display_debug_info(
                sql_file_path=sql_file,
                params=params,
                results=results,
                query_name=query_name
            )
    
    # Key Metrics Section
    try:
        # Calculate KPIs
        critical_pct = (priority_data[priority_data["priority"] == "Critical"]["ticket_count"].sum() / 
                      priority_data["ticket_count"].sum() * 100)
        
        # Calculate average first response time in minutes
        avg_response_time = first_response_data["avg_response_time_minutes"].mean()
        response_trend = get_smoothed_trend_data(first_response_data, "avg_response_time_minutes")
        
        # Calculate resolution rate
        resolution_rate = resolution_rate_data["resolution_rate"].mean()
        resolution_trend = get_smoothed_trend_data(resolution_rate_data, "resolution_rate")
        
        # Calculate customer effort score
        effort_score = customer_effort_data["customer_effort_score"].mean()
        effort_trend = get_smoothed_trend_data(customer_effort_data, "customer_effort_score")
        
        # Calculate channel effectiveness
        channel_effectiveness = channel_effectiveness_data["channel_effectiveness_score"].mean()
        effectiveness_trend = get_smoothed_trend_data(channel_effectiveness_data, "channel_effectiveness_score")
        
        # Convert average response time to hours and minutes for display
        if pd.isna(avg_response_time):
            avg_response_time_str = "N/A"
        else:
            avg_response_time_hours = int(avg_response_time // 60)
            avg_response_time_minutes_part = int(avg_response_time % 60)
            avg_response_time_str = f"{avg_response_time_hours}h {avg_response_time_minutes_part}m"

        kpis = [
            {
                "label": "First Response Time",
                "value": avg_response_time_str,
                "delta": -calculate_delta(response_trend),  # Negative because lower is better
                "help": """
                Average time to first response on support tickets.
                - Critical: 30min target
                - High: 2hr target
                - Medium: 8hr target
                - Low: 24hr target
                """,
                "trend_data": response_trend
            },
            {
                "label": "Resolution Rate",
                "value": f"{resolution_rate:.1f}%",
                "delta": calculate_delta(resolution_trend),
                "help": """
                Percentage of tickets resolved.
                - Includes all ticket priorities
                - Based on current ticket status
                - Week-over-week change shown
                """,
                "trend_data": resolution_trend
            },
            {
                "label": "Customer Effort Score",
                "value": f"{effort_score:.1f}",
                "delta": -calculate_delta(effort_trend),  # Negative because lower effort is better
                "help": """
                Composite score of customer effort required:
                - Priority level (40%)
                - Sentiment impact (30%)
                - Resolution time (30%)
                Lower score = less customer effort
                """,
                "trend_data": effort_trend
            },
            {
                "label": "Channel Effectiveness",
                "value": f"{channel_effectiveness:.1f}%",
                "delta": calculate_delta(effectiveness_trend),
                "help": """
                Overall effectiveness of support channels:
                - Resolution rate (40%)
                - Customer sentiment (40%)
                - Resolution time (20%)
                Higher score = more effective
                """,
                "trend_data": effectiveness_trend
            }
        ]
        
        # Render KPIs
        render_kpis(kpis, columns=4)
        
        # Add download button for KPI data
        kpi_data_for_json = [
            {
                "label": kpi["label"],
                "value": kpi["value"],
                "help": kpi["help"],
                "delta": kpi["delta"],
                "trend_data": kpi["trend_data"].tolist() if isinstance(kpi["trend_data"], pd.Series) else None
            }
            for kpi in kpis
        ]
        
        safe_download_button(
            data=json.dumps(kpi_data_for_json, indent=2, default=decimal_to_float),
            filename="support_kpi_data.json",
            label="⇓ Download KPI Data",
            mime_type="application/json",
            help_text="Download the current KPI data as JSON",
            key="download_kpi_data_support"
        )
        
    except KeyError as e:
        st.error(f"Error calculating KPIs: Missing column {str(e)}")
        if debug_mode:
            st.write("Available columns:", ticket_volume_data.columns.tolist())
        return
    
    # Ticket Volume Trend Section
    with st.expander("Ticket Volume Trend", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Ticket Volume Trend</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Track the volume of support tickets over time. Use this chart to identify patterns, spikes, or trends in support demand.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        if not ticket_volume_data.empty:
            # Get current theme
            theme = get_current_theme()

            # Calculate 7-day rolling average
            ticket_volume_data['rolling_avg_ticket_count'] = ticket_volume_data['ticket_count'].rolling(window=7, min_periods=1).mean()
            
            fig = go.Figure()

            # Add trace for daily ticket counts (as bars)
            fig.add_trace(go.Bar(
                x=ticket_volume_data['date'],
                y=ticket_volume_data['ticket_count'],
                name='Daily Tickets',
                marker_color=theme.get('primaryColor', '#1f77b4') # Use theme color or a default
            ))

            # Add trace for 7-day rolling average (as a line)
            fig.add_trace(go.Scatter(
                x=ticket_volume_data['date'],
                y=ticket_volume_data['rolling_avg_ticket_count'],
                mode='lines',
                name='7-Day Rolling Average',
                line=dict(color=theme.get('secondaryColor', '#ff7f0e'), width=2) # Use another theme color or default (e.g., orange)
            ))
            
            fig.update_layout(
                paper_bgcolor=theme['background'],
                plot_bgcolor=theme['background'],
                font=dict(color=theme['text']),
                margin=dict(t=20, l=0, r=0, b=0), # Adjusted top margin as chart title is removed
                height=400,
                xaxis=dict(
                    title_text='Date',
                    gridcolor=theme['border'],
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text'])
                ),
                yaxis=dict(
                    title_text='Number of Tickets',
                    gridcolor=theme['border'],
                    linecolor=theme['border'],
                    tickfont=dict(color=theme['text'])
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    bgcolor=theme.get('secondaryBackgroundColor', theme['background']), # Ensure legend bg matches theme
                    font=dict(color=theme['text'])
                ),
                barmode='overlay' # Ensure bars and lines overlay nicely if needed, though with one bar trace it's less critical
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Add download button for trend data
            safe_download_button(
                data=ticket_volume_data.to_csv(index=False),
                filename="ticket_volume.csv",
                label="⇓ Download Trend Data",
                mime_type="text/csv",
                help_text="Download the ticket volume data as CSV",
                key="download_trend_data_support"
            )
        else:
            st.info("No ticket volume data available for the selected period.")
    
    # Priority Breakdown Section
    with st.expander("Priority Breakdown", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Priority Breakdown</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Analyze the distribution of tickets across different priority levels. Use this to understand the urgency and severity of support requests.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        
        if not priority_data.empty:
            # Sort priorities by severity
            priority_order = ['Critical', 'High', 'Medium', 'Low']
            priority_data['priority'] = pd.Categorical(priority_data['priority'], categories=priority_order, ordered=True)
            priority_data = priority_data.sort_values('priority')
            # Define color map for priorities
            color_map = {
                'Critical': '#d62728',  # red
                'High': '#ff7f0e',     # orange
                'Medium': '#1f77b4',   # blue
                'Low': '#2ca02c'       # green
            }
            col1, col2 = st.columns(2)
            with col1:
                # Get current theme
                theme = get_current_theme()
                fig = px.pie(
                    priority_data,
                    values='ticket_count',
                    names='priority',
                    labels={'ticket_count': 'Number of Tickets', 'priority': 'Priority Level'},
                    color='priority',
                    color_discrete_map=color_map,
                    hole=0.5
                )
                fig.update_traces(textinfo='percent+label')
                fig.update_layout(
                    paper_bgcolor=theme['background'],
                    plot_bgcolor=theme['background'],
                    font=dict(color=theme['text'])
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                # Get current theme
                theme = get_current_theme()

                # Aggregate data for stacked bar chart
                bar_chart_data = priority_data.groupby(
                    ['priority', 'category'], as_index=False, observed=True
                )['ticket_count'].sum()

                fig = px.bar(
                    bar_chart_data,
                    x='priority',
                    y='ticket_count',
                    labels={'ticket_count': 'Number of Tickets', 'priority': 'Priority Level', 'category': 'Category'},
                    color='category', # Stack by category
                    text_auto=True,
                    category_orders={'priority': priority_order} # Ensure x-axis order
                )
                fig.update_layout(
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
                    ),
                    barmode='stack', # Ensure bars are stacked
                    showlegend=True # Show legend for categories
                )
                st.plotly_chart(fig, use_container_width=True)
            # Add download button for priority data
            safe_download_button(
                data=priority_data.to_csv(index=False),
                filename="priority_breakdown.csv",
                label="⇓ Download Priority Data",
                mime_type="text/csv",
                help_text="Download the priority breakdown data as CSV",
                key="download_priority_data_support"
            )
        else:
            st.info("No priority data available.")
    
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