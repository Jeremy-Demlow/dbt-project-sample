"""
Main Streamlit application entry point.
"""

import streamlit as st
import base64
from datetime import datetime, timedelta

# Set page config must be the first Streamlit command
st.set_page_config(
    page_title="Customer Intelligence Hub",
    page_icon="📊",
    layout="wide"
)

# Import required modules first
from utils.debug import render_global_debug_toggle
from utils.theme import initialize_theme, apply_theme, render_theme_toggle
from utils.snowflake_compatibility import initialize_snowflake_compatibility
from components import registry

# Initialize Snowflake compatibility features first
initialize_snowflake_compatibility()

def initialize_session_state():
    """Initialize and validate all session state variables."""
    # Initialize theme first
    initialize_theme()
    
    # Debug settings
    if 'debug' not in st.session_state:
        st.session_state.debug = {
            'enabled': False,
            'last_updated': datetime.now()
        }
    
    # Initialize filters
    if 'filters' not in st.session_state:
        default_end = datetime.now()
        default_start = default_end - timedelta(days=90)
        st.session_state.filters = {
            'start_date': default_start.strftime('%Y-%m-%d'),
            'end_date': default_end.strftime('%Y-%m-%d'),
            'personas': []  # Empty list by default
        }
    
    # Validate session state values
    validate_session_state()

def validate_session_state():
    """Validate session state values and reset to defaults if invalid."""
    # Validate debug settings
    if not isinstance(st.session_state.debug.get('enabled'), bool):
        st.session_state.debug['enabled'] = False

# Initialize session state first
initialize_session_state()

# Apply initial theme after session state is initialized
apply_theme()

# Render global debug toggle in sidebar
render_global_debug_toggle()

# Add theme toggle in sidebar
render_theme_toggle()

# Create a more impactful header with improved visual hierarchy
st.markdown("""
    <div class="card-primary" style="text-align: center; margin-top: -2rem;">
        <h1 class="heading-primary" style="margin-bottom: 0;">📊 Customer Intelligence Hub</h1>
        <p class="text-secondary" style="margin-bottom: 0; font-size: var(--font-size-lg);">
            Advanced analytics powered by Snowflake Cortex and dbt
        </p>
    </div>
""", unsafe_allow_html=True)

# Add 'Powered by' and logos anchored to the bottom center of the sidebar
with st.sidebar:
    # Add vertical space to push content to the bottom
    st.markdown("""
        <div style='flex:1; min-height: 100px;'></div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='text-align: center; margin-bottom: 0;'><h4>Powered by</h4></div>", unsafe_allow_html=True)
    
    # Create logo container with links
    try:
        import os
        # Get the directory of the current file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        assets_dir = os.path.join(current_dir, "assets")
        
        # Load images safely
        snowflake_logo_path = os.path.join(assets_dir, "snowflake-logo.png")
        dbt_logo_path = os.path.join(assets_dir, "dbt-labs-signature_tm_light.svg" if st.session_state.theme['dark_mode'] else "dbt-labs-logo.svg")
        
        snowflake_b64 = ""
        dbt_b64 = ""
        
        if os.path.exists(snowflake_logo_path):
            with open(snowflake_logo_path, "rb") as f:
                snowflake_b64 = base64.b64encode(f.read()).decode()
        
        if os.path.exists(dbt_logo_path):
            with open(dbt_logo_path, "rb") as f:
                dbt_b64 = base64.b64encode(f.read()).decode()
        
        st.markdown("""
            <div class="logo-container" style="display: flex; flex-direction: column; align-items: center; gap: 20px;">
                <a href="https://www.snowflake.com" target="_blank" class="logo-link">
                    <img src="data:image/png;base64,{}" alt="Snowflake" style="max-width: 120px;">
                </a>
                <a href="https://www.getdbt.com" target="_blank" class="logo-link">
                    <img src="data:image/svg+xml;base64,{}" alt="dbt" style="max-width: 120px;">
                </a>
            </div>
        """.format(snowflake_b64, dbt_b64), unsafe_allow_html=True)
        
    except Exception as e:
        # Fallback: Show text-based logos if images fail to load
        st.markdown("""
            <div class="logo-container" style="display: flex; flex-direction: column; align-items: center; gap: 20px;">
                <a href="https://www.snowflake.com" target="_blank" style="text-decoration: none;">
                    <div style="font-size: 1.2em; font-weight: bold; color: #29B5E8;">❄️ Snowflake</div>
                </a>
                <a href="https://www.getdbt.com" target="_blank" style="text-decoration: none;">
                    <div style="font-size: 1.2em; font-weight: bold; color: #FF694B;">🔧 dbt</div>
                </a>
            </div>
        """, unsafe_allow_html=True)
        
        # Log the error if debug mode is enabled
        if st.session_state.debug['enabled']:
            st.error(f"Logo loading error: {str(e)}")

# Add spacing before tabs
st.markdown('<div style="margin: var(--space-md) 0;"></div>', unsafe_allow_html=True)

# Create tabs for different dashboard views with improved styling
tabs = st.tabs([f"{component.icon} {component.display_name}" 
                for component in registry.get_all_components()])

# Render each dashboard component in its respective tab
for tab, component in zip(tabs, registry.get_all_components()):
    with tab:
        # Add consistent spacing within tabs
        st.markdown('<div style="margin: var(--space-sm) 0;"></div>', unsafe_allow_html=True)
        registry.render_component(
            component.name,
            st.session_state.filters,  # Use session state filters
            debug_mode=st.session_state.debug['enabled']
        )

# Custom CSS styles are now handled by the theme system and styles.css 