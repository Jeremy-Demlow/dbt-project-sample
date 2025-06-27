"""
Segmentation dashboard component showing customer segments and their characteristics.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.database import snowflake_conn
from utils.debug import display_debug_info
from utils.theme import get_current_theme
from utils.kpi_cards import render_kpis, render_simple_kpis
from utils.snowflake_compatibility import safe_download_button, clean_chart_data
import os

# --- KPI Data Loading Functions ---
@st.cache_data(ttl=300)
def load_combined_kpi_data() -> pd.DataFrame:
    """Load combined data for all KPIs."""
    query = "segmentation/kpi_combined_segmentation.sql"
    results = snowflake_conn.execute_query(query)
    df = pd.DataFrame(results)
    if st.session_state.get('debug_mode', False):
        display_debug_info(sql_file_path=query, params={}, results=df, query_name="Combined KPI Data")
    return df

# Removed old KPI data loading functions:
# load_dominant_persona_data
# load_high_value_customer_percentage_data
# load_high_value_churn_risk_share_data
# load_total_ltv_at_risk_data
# --- End KPI Data Loading Functions ---

def render_segmentation(filters: dict, debug_mode: bool = False) -> None:
    """Render the Segmentation & Value dashboard tab.
    
    Args:
        filters: Dictionary of global filter values.
        debug_mode: Boolean flag to control visibility of debug information
    """
    st.markdown("""
    <style>
    h2 {
        padding: 0 !important;
        margin: 0 !important;
    }
    /* Tooltip styles are handled by main styles.css */
    </style>
    <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem;">
        <h2>Customer Segmentation & Value</h2>
        <div class="tooltip">
            <span class="help-icon">?</span>
            <span class="tooltiptext">
                Explore customer segments, their characteristics, value, and migration patterns.
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # --- Load KPI Data ---
    with st.spinner("Loading KPI data..."):
        combined_kpi_df = load_combined_kpi_data()

    # --- Prepare and Render KPIs ---
    kpis_to_render = []

    if not combined_kpi_df.empty:
        # Extract data from the first (and only) row of the combined DataFrame
        kpi_data = combined_kpi_df.iloc[0]

        # 1. Dominant Persona
        persona_name = kpi_data.get('DOMINANT_PERSONA_NAME')
        persona_count = kpi_data.get('DOMINANT_PERSONA_COUNT')
        if pd.notna(persona_name) and pd.notna(persona_count):
            kpis_to_render.append({
                "label": f"Dominant Persona: {persona_name}",
                "value": f"{int(persona_count):,} Customers",
                "delta": 0, 
                "help": "The largest customer segment identified and the number of customers within it.",
                "trend_data": None
            })
        else:
            kpis_to_render.append({
                "label": "Dominant Persona", "value": "N/A", "delta": 0, 
                "help": "Could not load dominant persona data.", "trend_data": None
            })

        # 2. High-value Customer Percentage
        hv_customer_percentage = kpi_data.get('HIGH_VALUE_CUSTOMER_PERCENTAGE')
        if pd.notna(hv_customer_percentage):
            kpis_to_render.append({
                "label": "High-Value Customer %",
                "value": f"{hv_customer_percentage:.1f}%",
                "delta": 0, 
                "help": "Percentage of customers with a lifetime value greater than $1,000.",
                "trend_data": None
            })
        else:
            kpis_to_render.append({
                "label": "High-Value Customer %", "value": "N/A", "delta": 0, 
                "help": "Could not load high-value customer percentage.", "trend_data": None
            })
            
        # 3. Share of High-Value Customers in High Churn Risk
        hv_churn_share = kpi_data.get('SHARE_HIGH_VALUE_IN_HIGH_RISK')
        if pd.notna(hv_churn_share):
            kpis_to_render.append({
                "label": "High-Value Churn Risk %",
                "value": f"{hv_churn_share:.1f}%",
                "delta": 0, 
                "help": "Proportion of high-value customers (LTV >= $1000) who are also at high risk of churning.",
                "trend_data": None
            })
        else:
            kpis_to_render.append({
                "label": "High-Value Churn Risk %", "value": "N/A", "delta": 0,
                "help": "Could not load data for high-value customers in high churn risk.", "trend_data": None
            })

        # 4. Total LTV at Risk (High Churn Segment)
        total_ltv = kpi_data.get('TOTAL_LTV_AT_RISK')
        if pd.notna(total_ltv):
            kpis_to_render.append({
                "label": "LTV at Risk (High Churn)",
                "value": f"${total_ltv:,.0f}",
                "delta": 0, 
                "help": "Total lifetime value of customers currently classified with 'High' churn risk.",
                "trend_data": None
            })
        else:
            kpis_to_render.append({
                "label": "LTV at Risk (High Churn)", "value": "N/A", "delta": 0,
                "help": "Could not load data for total LTV at risk.", "trend_data": None
            })
    else: # If combined_kpi_df is empty
        for label, help_text in [
            ("Dominant Persona", "Could not load dominant persona data."),
            ("High-Value Customer %", "Could not load high-value customer percentage."),
            ("High-Value Churn Risk %", "Could not load data for high-value customers in high churn risk."),
            ("LTV at Risk (High Churn)", "Could not load data for total LTV at risk.")
        ]:
            kpis_to_render.append({
                "label": label, "value": "N/A", "delta": 0, 
                "help": help_text, "trend_data": None
            })
        
    if kpis_to_render:
        # Adapt KPIs for render_simple_kpis
        simple_kpis = []
        for kpi in kpis_to_render:
            simple_kpis.append({
                "label": kpi["label"],
                "value": kpi["value"],
                "help": kpi["help"],
                "timeframe": "Summary" # Using a generic timeframe as these are summary stats
            })
        render_simple_kpis(simple_kpis, columns=4)
    # --- End KPIs ---
    
    # --- Visual Inventory from STREAMLIT_PRD.md Section 4.5 ---
    
    with st.expander("Persona Distribution Analysis", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Persona Distribution</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Displays the number of customers belonging to each identified persona. Use this to understand the size of each customer group and identify dominant personas.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        persona_dist_query = "segmentation/persona_distribution.sql"
        with st.spinner("Loading persona distribution data..."):
            persona_dist_chart_data = pd.DataFrame(snowflake_conn.execute_query(persona_dist_query)) # No params
            
            if not persona_dist_chart_data.empty and 'PERSONA' in persona_dist_chart_data.columns and 'CUSTOMER_COUNT' in persona_dist_chart_data.columns:
                theme = get_current_theme()
                fig_persona_dist = px.bar(
                    persona_dist_chart_data,
                    x="PERSONA",
                    y="CUSTOMER_COUNT",
                    title="Customer Persona Distribution",
                    labels={"PERSONA": "Persona", "CUSTOMER_COUNT": "Number of Customers"},
                    color="PERSONA"
                )
                fig_persona_dist.update_layout(
                    paper_bgcolor=theme['background'],
                    plot_bgcolor=theme['background'],
                    font=dict(color=theme['text']),
                    xaxis=dict(gridcolor=theme['border'], linecolor=theme['border'], tickfont=dict(color=theme['text'])),
                    yaxis=dict(gridcolor=theme['border'], linecolor=theme['border'], tickfont=dict(color=theme['text']))
                )
                st.plotly_chart(fig_persona_dist, use_container_width=True)
                safe_download_button(
                    data=persona_dist_chart_data.to_csv(index=False),
                    filename="persona_distribution.csv",
                    label="⇓ Download Persona Distribution Data",
                    mime_type="text/csv",
                    help_text="Download the persona distribution data as CSV",
                    key="download_persona_distribution_segmentation"
                )
            else:
                st.info("No persona distribution data available.")
            if debug_mode:
                 display_debug_info(
                    sql_file_path=persona_dist_query, params={}, results=persona_dist_chart_data, query_name="Persona Distribution Chart"
                )

    with st.expander("Value Segment Radar", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Value Segment Radar</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Presents a multi-axis view comparing normalized key performance indicators (e.g., average purchase value, frequency) across different value segments (e.g., High, Medium, Low Value). Shapes leaning towards the outer edge on an axis indicate stronger performance for that metric in a segment.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        value_segment_metrics_query = "segmentation/value_segment_metrics.sql"
        with st.spinner("Loading value segment metrics..."):
            value_seg_radar_data = pd.DataFrame(snowflake_conn.execute_query(value_segment_metrics_query))

            if not value_seg_radar_data.empty and 'SEGMENT' in value_seg_radar_data.columns and \
               'METRIC_NAME' in value_seg_radar_data.columns and 'METRIC_VALUE' in value_seg_radar_data.columns:
                
                theme = get_current_theme()
                fig_radar = go.Figure()
                segments = value_seg_radar_data['SEGMENT'].unique()

                for segment_val in segments: # Renamed variable to avoid conflict
                    segment_data_df = value_seg_radar_data[value_seg_radar_data['SEGMENT'] == segment_val] # Renamed variable
                    fig_radar.add_trace(go.Scatterpolar(
                        r=segment_data_df['METRIC_VALUE'],
                        theta=segment_data_df['METRIC_NAME'],
                        fill='toself',
                        name=segment_val
                    ))
                
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(visible=True, range=[0, value_seg_radar_data['METRIC_VALUE'].max()]),
                        bgcolor=theme['background'],
                        angularaxis=dict(linecolor=theme['border'], gridcolor=theme['border'], tickfont=dict(color=theme['text'])),
                        radialaxis_gridcolor=theme['border'],
                        radialaxis_linecolor=theme['border'],
                        radialaxis_tickfont=dict(color=theme['text'])
                    ),
                    showlegend=True,
                    title="Value Segment Metrics Radar",
                    paper_bgcolor=theme['background'],
                    font=dict(color=theme['text']),
                    legend=dict(font=dict(color=theme['text']))
                )
                st.plotly_chart(fig_radar, use_container_width=True)
                safe_download_button(
                    data=value_seg_radar_data.to_csv(index=False),
                    filename="value_segment_metrics.csv",
                    label="⇓ Download Value Segment Metrics",
                    mime_type="text/csv",
                    help_text="Download the value segment metrics data as CSV",
                    key="download_value_segment_metrics_segmentation"
                )
            else:
                st.info("No data available for Value Segment Radar.")
            if debug_mode:
                display_debug_info(
                    sql_file_path=value_segment_metrics_query, params={}, results=value_seg_radar_data, query_name="Value Segment Radar"
                )
    
    with st.expander("Churn vs Upsell Potential", expanded=True):
        st.markdown('''
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            <h3 style="margin: 0; font-size: 1.2rem; font-weight: 600;">Churn vs Upsell Potential</h3>
            <div class="tooltip">
                <span class="help-icon">?</span>
                <span class="tooltiptext">
                    Shows a 2D density plot of customers based on their predicted churn likelihood and upsell potential scores. Darker areas indicate a higher concentration of customers. Helps identify at-risk high-potential customers or safe low-potential ones.
                </span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        engagement_query = "segmentation/churn_vs_upsell.sql"
        with st.spinner("Loading churn vs upsell data..."):
            churn_upsell_data = pd.DataFrame(snowflake_conn.execute_query(engagement_query))
            
            if not churn_upsell_data.empty and 'CHURN_SCORE' in churn_upsell_data.columns and 'UPSELL_POTENTIAL' in churn_upsell_data.columns:
                theme = get_current_theme()
                fig_density = px.density_heatmap(
                    churn_upsell_data,
                    x="CHURN_SCORE",
                    y="UPSELL_POTENTIAL",
                    title="Churn Score vs. Upsell Potential Density",
                    labels={"CHURN_SCORE": "Churn Likelihood Score", "UPSELL_POTENTIAL": "Upsell Potential Score"}
                )
                fig_density.update_layout(
                    paper_bgcolor=theme['background'],
                    plot_bgcolor=theme['background'],
                    font=dict(color=theme['text']),
                    xaxis=dict(gridcolor=theme['border'], linecolor=theme['border'], tickfont=dict(color=theme['text'])),
                    yaxis=dict(gridcolor=theme['border'], linecolor=theme['border'], tickfont=dict(color=theme['text'])),
                    coloraxis_colorbar=dict(tickfont=dict(color=theme['text']))
                )
                st.plotly_chart(fig_density, use_container_width=True)
                safe_download_button(
                    data=churn_upsell_data.to_csv(index=False),
                    filename="churn_upsell_data.csv",
                    label="⇓ Download Churn vs Upsell Data",
                    mime_type="text/csv",
                    help_text="Download the churn vs upsell potential data as CSV",
                    key="download_churn_upsell_data_segmentation"
                )
            else:
                st.info("No data available for Churn vs Upsell Density.")
            if debug_mode:
                display_debug_info(
                    sql_file_path=engagement_query, params={}, results=churn_upsell_data, query_name="Churn vs Upsell Density"
                )
            
    # Removed Segment Explorer expander block

    # Removed the "Additional Segment Visualizations" expander block
    
    # Removed the separate "Download Data (PRD Aligned)" and other "Download Data" subheader sections
    # as buttons are now integrated into their respective visual sections/expanders. 