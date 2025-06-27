"""
Helper functions for rendering KPI metrics in a consistent format.
"""

from typing import Dict, List, Optional, Union
import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt
import io
import base64

def clean_trend_data(series: Optional[pd.Series]) -> Optional[pd.Series]:
    """Clean trend data by handling NaN, inf, and out-of-range values.
    
    Args:
        series: Input pandas Series or None
        
    Returns:
        pd.Series: Cleaned series with valid float values, or None if input is None
    """
    if series is None:
        return None
        
    # Convert to float
    series = series.astype(float)
    
    # Replace inf/-inf with NaN
    series = series.replace([np.inf, -np.inf], np.nan)
    
    # Drop NaN values
    series = series.dropna()
    
    # Clip extreme values to prevent JSON serialization issues
    max_float = 1e38  # Maximum value that can be safely serialized to JSON
    series = series.clip(-max_float, max_float)
    
    return series

def create_sparkline(trend_data: Optional[pd.Series]) -> str:
    """Create a base64 encoded sparkline chart.
    
    Args:
        trend_data: Pandas Series with trend data or None
        
    Returns:
        str: Base64 encoded PNG image, or empty string if no data
    """
    if trend_data is None or trend_data.empty:
        return ""
        
    # Create the Matplotlib figure
    fig, ax = plt.subplots(figsize=(4, 2))
    ax.plot(trend_data.values, color='#3b82f6')
    ax.fill_between(range(len(trend_data)), trend_data.values, alpha=0.3, color='#3b82f6')
    ax.axis('off')
    
    # Save the figure to a BytesIO buffer
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", transparent=True)
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode("utf-8")
    buf.close()
    plt.close(fig)
    
    return img_base64

def render_kpis(
    kpis: List[Dict[str, Union[str, pd.Series, float]]],
    columns: int = 4,
    delta_color: str = "normal"
) -> None:
    """Render a row of KPI metric cards.
    
    Args:
        kpis: List of dictionaries containing KPI data
            Each dictionary should have:
            - 'label': Display name of the KPI
            - 'value': Current value
            - 'delta': Optional change from previous period
            - 'help': Optional tooltip text
            - 'timeframe': Optional timeframe label
            - 'trend_data': Optional pandas Series with trend data
        columns: Number of columns to display (default: 4)
        delta_color: Color of the delta indicator
            Options: "normal", "inverse", "off"
    """
    # Create columns for KPI cards
    cols = st.columns(columns)
    
    # Add custom CSS for KPI cards
    st.markdown("""
    <style>
    div[data-testid="column"] {
        background: var(--background-color);
        border-radius: 1rem;
        padding: 0 !important;
        margin: 0 0.5rem;
    }
    div[data-testid="column"]:first-child {
        margin-left: 0;
    }
    div[data-testid="column"]:last-child {
        margin-right: 0;
    }
    .kpi-card {
        background: var(--background-color);
        padding: 1.5rem;
        border-radius: 1rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 25px rgba(0,0,0,0.15);
    }
    .kpi-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .kpi-label {
        font-size: 0.875rem;
        color: var(--text-color-secondary);
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: var(--text-color);
        margin: 0.5rem 0;
        text-align: center;
        line-height: 1.2;
    }
    .kpi-badge {
        background: #3b82f6;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 500;
        white-space: nowrap;
    }
    .kpi-trend {
        display: flex;
        align-items: center;
        margin-top: auto;
        font-weight: 600;
        font-size: 0.875rem;
        text-align: center;
        justify-content: center;
        gap: 0.25rem;
        padding-top: 0.5rem;
    }
    .kpi-trend.positive {
        color: #16a34a !important;
    }
    .kpi-trend.negative {
        color: #dc2626 !important;
    }
    .kpi-trend-text {
        color: var(--text-color-secondary);
        font-weight: normal;
        font-size: 0.875rem;
    }
    .kpi-chart {
        margin: 1rem 0;
        text-align: center;
        min-height: 40px;
        flex-grow: 1;
    }
    .help-icon {
        color: var(--text-color-secondary);
        cursor: help;
        font-size: 1rem;
        width: 1.25rem;
        height: 1.25rem;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 50%;
        background: var(--background-color-secondary, #f3f4f6);
        transition: all 0.2s ease;
    }
    .help-icon:hover {
        background: var(--background-color-tertiary, #e5e7eb);
    }
    /* Tooltip styles are defined in main styles.css to avoid conflicts */
    @media (max-width: 768px) {
        div[data-testid="column"] {
            margin: 0.5rem 0;
        }
        .kpi-card {
            padding: 1rem;
        }
        .kpi-value {
            font-size: 1.5rem;
        }
        .kpi-trend {
            font-size: 0.75rem;
        }
        .kpi-trend-text {
            font-size: 0.75rem;
        }
        /* Tooltip styles are handled by main styles.css */
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Render each KPI in its column
    for i, kpi in enumerate(kpis):
        with cols[i % columns]:
            # Prepare trend data if available
            trend_data = kpi.get('trend_data')
            sparkline_img = ""
            if trend_data is not None:
                cleaned_data = clean_trend_data(trend_data)
                if cleaned_data is not None and not cleaned_data.empty:
                    sparkline_img = create_sparkline(cleaned_data)
            
            # Determine trend class and arrow
            delta = kpi.get('delta', 0)
            trend_class = "positive" if delta >= 0 else "negative"
            trend_arrow = "↑" if delta >= 0 else "↓"
            
            # Render the card
            card_html = f"""
            <div class="kpi-card">
                <div class="kpi-header">
                    <div class="kpi-label">
                        {kpi['label']}
                        <div class="tooltip">
                            <span class="help-icon">?</span>
                            <span class="tooltiptext">{kpi.get('help', '')}</span>
                        </div>
                    </div>
                    <div class="kpi-badge">Weekly</div>
                </div>
                <div class="kpi-value">{kpi["value"]}</div>
                {f'<div class="kpi-chart"><img src="data:image/png;base64,{sparkline_img}" style="width:100%; height:auto;" /></div>' if sparkline_img else ''}
                <div class="kpi-trend {trend_class}">
                    {trend_arrow} {abs(delta):.1f}% 
                    <span class="kpi-trend-text">since last week</span>
                </div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)

def render_simple_kpis(
    kpis: List[Dict[str, Union[str, float]]],
    columns: int = 4
) -> None:
    """Render a row of KPI metric cards without trend information.
    
    Args:
        kpis: List of dictionaries containing KPI data
            Each dictionary should have:
            - 'label': Display name of the KPI
            - 'value': Current value
            - 'help': Optional tooltip text
            - 'timeframe': Optional timeframe label (used as badge)
        columns: Number of columns to display (default: 4)
    """
    # Create columns for KPI cards
    cols = st.columns(columns)
    
    # Add custom CSS for KPI cards (can reuse or adapt existing if needed)
    # For simplicity, this example reuses the existing CSS from render_kpis.
    # If significantly different styling is needed, a new style block can be added.
    # Ensure the CSS in render_kpis is general enough or add specific styles here.
    # Adding a slightly modified CSS for simple cards
    st.markdown("""
    <style>
    /* General column styling from render_kpis - can be shared or redefined */
    div[data-testid="column"].simple-kpi-column { /* Add a specific class if needed */
        background: var(--background-color);
        border-radius: 1rem;
        padding: 0 !important;
        margin: 0 0.5rem;
    }
    div[data-testid="column"].simple-kpi-column:first-child {
        margin-left: 0;
    }
    div[data-testid="column"].simple-kpi-column:last-child {
        margin-right: 0;
    }
    .simple-kpi-card {
        background: var(--background-color);
        padding: 1.5rem;
        border-radius: 1rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between; /* Align items for cards without trends */
    }
    .simple-kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 25px rgba(0,0,0,0.15);
    }
    .simple-kpi-header { /* Renamed for clarity */
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    .simple-kpi-label { /* Renamed */
        font-size: 0.875rem;
        color: var(--text-color-secondary);
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .simple-kpi-value { /* Renamed */
        font-size: 2rem;
        font-weight: bold;
        color: var(--text-color);
        margin: 0.5rem 0;
        text-align: center;
        line-height: 1.2;
        flex-grow: 1; /* Allow value to take more space */
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .simple-kpi-badge { /* Renamed */
        background: #3b82f6; /* Example color, can be dynamic */
        color: white !important; /* Ensure white text */
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 500;
        white-space: nowrap;
    }
    /* Tooltip and help icon styling is handled by main styles.css */
     @media (max-width: 768px) {
        div[data-testid="column"].simple-kpi-column {
            margin: 0.5rem 0;
        }
        .simple-kpi-card {
            padding: 1rem;
        }
        .simple-kpi-value {
            font-size: 1.5rem;
        }
        /* Tooltip styles are handled by main styles.css */
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Render each KPI in its column
    for i, kpi in enumerate(kpis):
        with cols[i % columns]:
            badge_text = kpi.get('timeframe', 'Info') # Default badge text
            
            # Render the card
            card_html = f"""
            <div class="simple-kpi-card">
                <div class="simple-kpi-header">
                    <div class="simple-kpi-label">
                        {kpi['label']}
                        <div class="tooltip">
                            <span class="help-icon">?</span>
                            <span class="tooltiptext">{kpi.get('help', '')}</span>
                        </div>
                    </div>
                    <div class="simple-kpi-badge">{badge_text}</div>
                </div>
                <div class="simple-kpi-value">{kpi["value"]}</div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)