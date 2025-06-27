"""
Snowflake Streamlit Environment Compatibility Utilities

This module provides utilities to handle browser console errors and compatibility issues
when running Streamlit applications in Snowflake's managed environment.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional, Any, Dict
import warnings

# Suppress warnings that commonly appear in Snowflake environment
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

def is_snowflake_environment() -> bool:
    """
    Detect if running in Snowflake Streamlit environment
    
    Returns:
        bool: True if running in Snowflake, False otherwise
    """
    try:
        import _snowflake
        return True
    except ImportError:
        return False

def clean_chart_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data for safe chart rendering in Snowflake environment
    
    Fixes issues like:
    - Infinite extent warnings for percentage fields
    - NaN values causing chart failures
    - Invalid numeric ranges
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame safe for chart rendering
    """
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # Replace infinite values with NaN
    df_clean.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # Handle percentage columns specifically (common source of infinite extent errors)
    percentage_cols = [col for col in df_clean.columns if 'PERCENTAGE' in col.upper()]
    for col in percentage_cols:
        if col in df_clean.columns:
            # Clamp percentages to reasonable range (0-100)
            df_clean[col] = df_clean[col].clip(0, 100)
            # Fill NaN with 0 for percentages
            df_clean[col] = df_clean[col].fillna(0)
    
    # Handle numeric columns
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col not in percentage_cols:  # Skip already processed percentage columns
            if df_clean[col].isna().all():
                # If all values are NaN, fill with 0
                df_clean[col] = 0
            else:
                # Fill NaN with median for numeric columns
                median_val = df_clean[col].median()
                if pd.isna(median_val):
                    df_clean[col] = df_clean[col].fillna(0)
                else:
                    df_clean[col] = df_clean[col].fillna(median_val)
    
    # Handle start/end columns that commonly cause infinite extent issues
    start_end_cols = [col for col in df_clean.columns if col.upper().endswith(('_START', '_END'))]
    for col in start_end_cols:
        if pd.api.types.is_numeric_dtype(df_clean[col]):
            # Ensure start/end values are finite
            df_clean[col] = df_clean[col].clip(-1e10, 1e10)  # Reasonable numeric range
            df_clean[col] = df_clean[col].fillna(0)
    
    return df_clean

def safe_download_button(data: Any, filename: str, label: str, mime_type: str = "text/csv", 
                        key: Optional[str] = None, help_text: Optional[str] = None) -> None:
    """
    Environment-aware download functionality that works in both Snowflake and standalone Streamlit
    
    In Snowflake: Creates a button that displays data in a text area for manual copy/paste
    In Standalone: Uses standard st.download_button
    
    Args:
        data: Data to download (string or bytes)
        filename: Suggested filename
        label: Button label
        mime_type: MIME type for the data
        key: Unique key for the widget
        help_text: Help text to display
    """
    button_key = key or f"download_{filename.replace('.', '_').replace(' ', '_')}"
    
    if is_snowflake_environment():
        # Snowflake-compatible download using text area
        if st.button(label, key=button_key, help=help_text):
            st.session_state[f"data_{filename}"] = data
            st.success("📋 Data ready! Copy from the text area below and save to your computer:")
            
            # Determine height based on data size
            if isinstance(data, str):
                line_count = data.count('\n')
                height = min(max(line_count * 20 + 50, 150), 400)  # Between 150-400px
            else:
                height = 200
            
            st.text_area(
                f"Copy this data and save as {filename}:",
                data,
                height=height,
                key=f"textarea_{filename}",
                help="Select all (Ctrl+A), copy (Ctrl+C), then paste into a text file"
            )
    else:
        # Standard Streamlit download button
        st.download_button(
            label=label,
            data=data,
            file_name=filename,
            mime=mime_type,
            key=button_key,
            help=help_text
        )

def create_safe_plotly_chart(data: pd.DataFrame, chart_type: str = "bar", 
                           title: Optional[str] = None, **kwargs) -> None:
    """
    Create Plotly charts with comprehensive error handling for Snowflake environment
    
    Args:
        data: DataFrame to visualize
        chart_type: Type of chart to create ('bar', 'line', 'scatter', etc.)
        title: Optional chart title
        **kwargs: Additional arguments passed to Plotly functions
    """
    try:
        # Clean the data first
        clean_data = clean_chart_data(data)
        
        if clean_data.empty:
            st.info("📊 No data available for visualization")
            return
        
        # Create chart based on type
        fig = None
        
        if chart_type == "bar" and len(clean_data.columns) >= 2:
            fig = px.bar(clean_data, x=clean_data.columns[0], y=clean_data.columns[1], 
                        title=title, **kwargs)
        elif chart_type == "line" and len(clean_data.columns) >= 2:
            fig = px.line(clean_data, x=clean_data.columns[0], y=clean_data.columns[1], 
                         title=title, **kwargs)
        elif chart_type == "scatter" and len(clean_data.columns) >= 2:
            fig = px.scatter(clean_data, x=clean_data.columns[0], y=clean_data.columns[1], 
                           title=title, **kwargs)
        elif chart_type == "pie" and len(clean_data.columns) >= 2:
            fig = px.pie(clean_data, names=clean_data.columns[0], values=clean_data.columns[1], 
                        title=title, **kwargs)
        else:
            st.warning(f"⚠️ Cannot create {chart_type} chart with available data structure")
            st.info("📋 Displaying data table instead:")
            st.dataframe(clean_data, use_container_width=True)
            return
        
        if fig:
            # Apply Snowflake-safe styling
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
                plot_bgcolor='rgba(0,0,0,0)',   # Transparent plot area
                margin=dict(t=60, l=60, r=60, b=60),  # Adequate margins
                title=dict(x=0.5, xanchor='center') if title else None,  # Center title
                font=dict(size=12),  # Readable font size
                showlegend=True if 'color' in kwargs else False
            )
            
            # Ensure axes are properly configured
            fig.update_xaxes(showgrid=True, gridcolor='rgba(128,128,128,0.2)')
            fig.update_yaxes(showgrid=True, gridcolor='rgba(128,128,128,0.2)')
            
            # Display the chart
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{chart_type}_{hash(str(clean_data.columns))}")
        
    except Exception as e:
        st.error(f"❌ Chart rendering error: {str(e)}")
        st.info("📋 Displaying data table as fallback:")
        st.dataframe(data, use_container_width=True)

def suppress_console_warnings() -> None:
    """
    Suppress common console warnings that appear in Snowflake environment
    
    This includes DataDog SDK warnings and other non-critical browser messages
    """
    if is_snowflake_environment():
        st.markdown("""
        <script>
        // Suppress common console warnings in Snowflake environment
        (function() {
            const originalWarn = console.warn;
            const originalError = console.error;
            
            console.warn = function(...args) {
                const message = args.join(' ');
                // Filter out known non-critical warnings
                if (!message.includes('Datadog Browser SDK') && 
                    !message.includes('Permissions-Policy') &&
                    !message.includes('ambient-light-sensor') &&
                    !message.includes('display-capture')) {
                    originalWarn.apply(console, args);
                }
            };
            
            console.error = function(...args) {
                const message = args.join(' ');
                // Filter out CSP errors that don't affect functionality
                if (!message.includes('Content Security Policy') ||
                    message.includes('critical')) {
                    originalError.apply(console, args);
                }
            };
        })();
        </script>
        """, unsafe_allow_html=True)

def handle_connection_issues() -> bool:
    """
    Handle connection issues gracefully in Snowflake environment
    
    Returns:
        bool: True if connection is healthy, False otherwise
    """
    try:
        # Test connection with a simple query
        if is_snowflake_environment():
            # In Snowflake environment, test the connection
            conn = st.connection("snowflake")
            result = conn.query("SELECT 1 as test", ttl=0)
            return len(result) > 0
        else:
            # In standalone environment, assume connection is fine
            return True
    except Exception as e:
        st.error("⚠️ Connection issue detected. Please refresh the page.")
        st.info("If issues persist, contact your Snowflake administrator.")
        
        # Show error details in debug mode
        if st.secrets.get("debug_mode", False):
            st.exception(e)
        
        return False

def safe_dataframe_display(df: pd.DataFrame, title: Optional[str] = None, 
                          max_rows: int = 1000) -> None:
    """
    Safely display DataFrame with size limits for Snowflake environment
    
    Args:
        df: DataFrame to display
        title: Optional title
        max_rows: Maximum rows to display (prevents performance issues)
    """
    if title:
        st.subheader(title)
    
    if df.empty:
        st.info("📋 No data to display")
        return
    
    # Clean the data
    clean_df = clean_chart_data(df)
    
    # Limit rows for performance
    if len(clean_df) > max_rows:
        st.warning(f"⚠️ Showing first {max_rows} rows of {len(clean_df)} total rows")
        display_df = clean_df.head(max_rows)
    else:
        display_df = clean_df
    
    # Display with proper formatting
    st.dataframe(
        display_df, 
        use_container_width=True,
        hide_index=True
    )
    
    # Add summary info
    st.caption(f"📊 {len(clean_df)} rows × {len(clean_df.columns)} columns")

def initialize_snowflake_compatibility() -> None:
    """
    Initialize Snowflake compatibility features
    
    Call this at the beginning of your Streamlit app to set up all compatibility features
    """
    # Suppress console warnings
    suppress_console_warnings()
    
    # Check connection health
    if not handle_connection_issues():
        st.stop()
    
    # Set page config optimizations for Snowflake
    if is_snowflake_environment():
        # Add custom CSS for better Snowflake compatibility
        st.markdown("""
        <style>
        /* Improve chart rendering in Snowflake */
        .js-plotly-plot .plotly .modebar {
            background: rgba(255, 255, 255, 0.7) !important;
        }
        
        /* Better button styling */
        .stButton > button {
            background-color: #0066cc;
            color: white;
            border-radius: 4px;
            border: none;
            padding: 0.5rem 1rem;
        }
        
        .stButton > button:hover {
            background-color: #0052a3;
        }
        
        /* Improve text area visibility */
        .stTextArea > div > div > textarea {
            font-family: 'Courier New', monospace;
            font-size: 12px;
        }
        </style>
        """, unsafe_allow_html=True)

# Convenience function for common usage pattern
def render_safe_section(data: pd.DataFrame, title: str, chart_type: str = "bar", 
                       enable_download: bool = True) -> None:
    """
    Render a complete section with title, chart, and optional download
    
    Args:
        data: DataFrame to visualize
        title: Section title
        chart_type: Type of chart to create
        enable_download: Whether to include download functionality
    """
    st.subheader(title)
    
    if not data.empty:
        # Create the chart
        create_safe_plotly_chart(data, chart_type, title=title)
        
        # Add download option
        if enable_download:
            safe_download_button(
                data=data.to_csv(index=False),
                filename=f"{title.lower().replace(' ', '_')}.csv",
                label="⇓ Download Data",
                key=f"download_{title.lower().replace(' ', '_')}"
            )
    else:
        st.info("📊 No data available for this section") 