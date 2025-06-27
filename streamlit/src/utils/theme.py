"""
Theme management module for the Streamlit application.
"""

import streamlit as st
from datetime import datetime
import logging
import os

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Theme configuration
THEME_CONFIG = {
    'light': {
        'primary': '#2563EB',  # indigo-600
        'secondary': '#3b82f6',  # indigo-500
        'background': '#ffffff',
        'background_secondary': '#f3f4f6',
        'text': '#111827',
        'text_secondary': '#374151',
        'border': '#e5e7eb',
        'hover': '#f9fafb',
        'button_text': '#f9fafb',  # Changed from '#111827' to white
        'trend_positive': '#3b82f6',  # indigo-500
        'trend_negative': '#1d4ed8',   # indigo-700
        'toggle': '#3b82f6'  # indigo-500
    },
    'dark': {
        'primary': '#3b82f6',  # indigo-500
        'secondary': '#3b82f6',  # indigo-500 (matching light theme)
        'background': '#1f2937',
        'background_secondary': '#374151',
        'text': '#f9fafb',
        'text_secondary': '#d1d5db',
        'border': '#4b5563',
        'hover': '#4b5563',
        'button_text': '#f9fafb',  # white for dark mode
        'trend_positive': '#3b82f6',  # indigo-500
        'trend_negative': '#1d4ed8',   # indigo-700
        'toggle': '#3b82f6'  # indigo-500
    }
}

def debug_session_state():
    """Debug function to inspect session state."""
    logger.debug("Current session state:")
    for key, value in st.session_state.items():
        logger.debug(f"  {key}: {value}")

def initialize_theme():
    """Initialize theme settings in session state."""
    logger.debug("Initializing theme...")
    debug_session_state()
    
    # Initialize theme state if it doesn't exist
    if 'theme' not in st.session_state:
        logger.debug("Theme not in session state, creating new theme state")
        st.session_state.theme = {
            'dark_mode': False,
            'high_contrast': False,
            'reduced_motion': False,
            'last_updated': datetime.now()
        }
    
    # Ensure the theme state is properly initialized
    if not isinstance(st.session_state.theme.get('dark_mode'), bool):
        logger.debug("Invalid dark_mode type, resetting to False")
        st.session_state.theme['dark_mode'] = False
        st.session_state.theme['last_updated'] = datetime.now()
    
    # Initialize new accessibility options if they don't exist
    if 'high_contrast' not in st.session_state.theme:
        st.session_state.theme['high_contrast'] = False
    if 'reduced_motion' not in st.session_state.theme:
        st.session_state.theme['reduced_motion'] = False
    
    logger.debug(f"Final theme state: {st.session_state.theme}")

def get_current_theme():
    """Get the current theme configuration."""
    logger.debug("Getting current theme...")
    
    # Ensure theme state exists
    if 'theme' not in st.session_state:
        logger.debug("Theme not found in session state, initializing")
        initialize_theme()
    
    theme_mode = 'dark' if st.session_state.theme['dark_mode'] else 'light'
    logger.debug(f"Current theme mode: {theme_mode}")
    return THEME_CONFIG[theme_mode]

def load_custom_css():
    """Load custom CSS file for enhanced styling."""
    css_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "styles.css")
    
    if os.path.exists(css_file_path):
        with open(css_file_path, "r") as f:
            css_content = f.read()
        
        st.markdown(f"""
            <style>
                {css_content}
            </style>
        """, unsafe_allow_html=True)
    else:
        logger.warning(f"CSS file not found at {css_file_path}")

def apply_theme():
    """Apply the current theme to the application."""
    logger.debug("Applying theme...")
    theme = get_current_theme()
    
    # Load custom CSS first
    load_custom_css()
    
    # Debug the theme being applied
    logger.debug(f"Applying theme colors: {theme}")
    
    # Get the current theme mode
    theme_mode = 'dark' if st.session_state.theme['dark_mode'] else 'light'
    
    # Base styles with higher specificity and data-theme attribute
    css = f"""
        <style>
            /* Set theme attribute on body */
            body {{
                data-theme: {theme_mode};
            }}
            
            html[data-theme="{theme_mode}"], 
            body[data-theme="{theme_mode}"],
            .stApp[data-theme="{theme_mode}"] {{
                --primary-color: {theme['primary']} !important;
                --secondary-color: {theme['secondary']} !important;
                --background-color: {theme['background']} !important;
                --background-color-secondary: {theme['background_secondary']} !important;
                --text-color: {theme['text']} !important;
                --text-color-secondary: {theme['text_secondary']} !important;
                --border-color: {theme['border']} !important;
                --hover-color: {theme['hover']} !important;
                --button-text-color: {theme['button_text']} !important;
                --trend-positive-color: {theme['trend_positive']} !important;
                --trend-negative-color: {theme['trend_negative']} !important;
                --toggle-color: {theme['toggle']} !important;
                --streamlit-primary: #3b82f6 !important;
                --font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif !important;
            }}
            
            /* Root variables with !important */
            :root {{
                --primary-color: {theme['primary']} !important;
                --secondary-color: {theme['secondary']} !important;
                --background-color: {theme['background']} !important;
                --background-color-secondary: {theme['background_secondary']} !important;
                --text-color: {theme['text']} !important;
                --text-color-secondary: {theme['text_secondary']} !important;
                --border-color: {theme['border']} !important;
                --hover-color: {theme['hover']} !important;
                --button-text-color: {theme['button_text']} !important;
                --trend-positive-color: {theme['trend_positive']} !important;
                --trend-negative-color: {theme['trend_negative']} !important;
                --toggle-color: {theme['toggle']} !important;
                --streamlit-primary: #3b82f6 !important;  /* Override default red */
                --font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif !important;
            }}

            /* Import Poppins font */
            @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');

            /* Apply font family globally */
            * {{
                font-family: var(--font-family) !important;
            }}

            /* Heading styles */
            h1 {{
                font-size: 2.5rem !important;
                font-weight: 700 !important;
                margin-bottom: 1.5rem !important;
            }}

            h2 {{
                font-size: 1.75rem !important;
                font-weight: 600 !important;
                margin-bottom: 1rem !important;
            }}

            /* Adjust Streamlit heading container padding */
            [data-testid="stHeadingWithActionElements"] {{
                padding-bottom: 0 !important;
                margin-bottom: 0 !important;
            }}

            /* Override Streamlit's default theme with higher specificity */
            .stApp, .stApp > div, .stApp > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Main content area with higher specificity */
            .main .block-container, 
            .main .block-container > div, 
            .main .block-container > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Sidebar with higher specificity */
            section[data-testid="stSidebar"],
            section[data-testid="stSidebar"] > div,
            section[data-testid="stSidebar"] > div > div,
            section[data-testid="stSidebar"] .stVerticalBlock,
            section[data-testid="stSidebar"] .stElementContainer,
            section[data-testid="stSidebar"] .stCheckbox,
            section[data-testid="stSidebar"] .stMarkdown,
            section[data-testid="stSidebar"] .stMarkdown > div,
            section[data-testid="stSidebar"] .stMarkdown > div > div,
            section[data-testid="stSidebar"] .stVerticalBlock > div,
            section[data-testid="stSidebar"] .stElementContainer > div,
            section[data-testid="stSidebar"] .stCheckbox > div,
            section[data-testid="stSidebar"] .stMarkdown > div > div > div {{
                background-color: var(--background-color-secondary) !important;
                color: var(--text-color) !important;
            }}

            /* Logo container in sidebar */
            .logo-container {{
                display: flex !important;
                justify-content: center !important;
                gap: 20px !important;
                margin-top: -10px !important;
                padding-top: 0 !important;
                background-color: var(--background-color-secondary) !important;
            }}

            /* Logo links */
            .logo-link {{
                opacity: 0.7 !important;
                transition: opacity 0.3s ease !important;
                background-color: var(--background-color-secondary) !important;
                padding: 0.5rem !important;
                border-radius: 0.5rem !important;
            }}

            .logo-link:hover {{
                opacity: 1 !important;
            }}

            /* Logo images */
            .logo-link img {{
                height: 60px !important;
                width: auto !important;
                object-fit: contain !important;
                background-color: var(--background-color-secondary) !important;
            }}

            /* Powered by text */
            section[data-testid="stSidebar"] h4 {{
                text-align: center !important;
                margin-bottom: 0 !important;
                background-color: var(--background-color-secondary) !important;
            }}

            /* Vertical space */
            section[data-testid="stSidebar"] .stMarkdown > div > div > div[style*="flex: 1"] {{
                background-color: var(--background-color-secondary) !important;
            }}

            /* Text elements with higher specificity */
            h1, h2, h3, h4, h5, h6, p, div, span, label,
            .stMarkdown, .stMarkdown > div, .stMarkdown > div > div {{
                color: var(--text-color) !important;
            }}

            /* Input elements with higher specificity */
            .stTextInput>div>div>input,
            .stNumberInput>div>div>input,
            .stSelectbox>div>div>select,
            .stTextArea>div>div>textarea,
            .stTextInput>div>div>input:focus,
            .stNumberInput>div>div>input:focus,
            .stSelectbox>div>div>select:focus,
            .stTextArea>div>div>textarea:focus {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Buttons with higher specificity */
            .stButton>button,
            .stButton>button:hover,
            .stButton>button:focus,
            .stButton>button:active,
            .stDownloadButton>button,
            .stDownloadButton>button:hover,
            .stDownloadButton>button:focus,
            .stDownloadButton>button:active,
            .element-container .stButton>button,
            .element-container .stButton>button:hover,
            .element-container .stButton>button:focus,
            .element-container .stButton>button:active,
            .element-container .stDownloadButton>button,
            .element-container .stDownloadButton>button:hover,
            .element-container .stDownloadButton>button:focus,
            .element-container .stDownloadButton>button:active {{
                background-color: var(--primary-color) !important;
                color: var(--button-text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Button text color override */
            .stButton>button p,
            .stDownloadButton>button p,
            .element-container .stButton>button p,
            .element-container .stDownloadButton>button p,
            .st-emotion-cache-zaw6nw p,
            .st-emotion-cache-zaw6nw .st-emotion-cache-p7i6r9 p,
            .st-emotion-cache-zaw6nw .st-emotion-cache-p7i6r9,
            [data-testid="stBaseButton"] p,
            [data-testid="stBaseButton"] .st-emotion-cache-p7i6r9 p,
            [data-testid="stBaseButton"] .st-emotion-cache-p7i6r9,
            [data-testid="stBaseButton-secondary"] p,
            [data-testid="stBaseButton-secondary"] .st-emotion-cache-p7i6r9 p,
            [data-testid="stBaseButton-secondary"] .st-emotion-cache-p7i6r9 {{
                color: var(--button-text-color) !important;
            }}

            /* All button styling */
            [data-testid="stBaseButton"],
            [data-testid="stBaseButton"]:hover,
            [data-testid="stBaseButton"]:focus,
            [data-testid="stBaseButton"]:active,
            [data-testid="stBaseButton-secondary"],
            [data-testid="stBaseButton-secondary"]:hover,
            [data-testid="stBaseButton-secondary"]:focus,
            [data-testid="stBaseButton-secondary"]:active,
            .st-emotion-cache-zaw6nw,
            .st-emotion-cache-zaw6nw:hover,
            .st-emotion-cache-zaw6nw:focus,
            .st-emotion-cache-zaw6nw:active {{
                background-color: var(--background-color-secondary) !important;
                color: var(--button-text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Primary button styling */
            [data-testid="stBaseButton-primary"],
            [data-testid="stBaseButton-primary"]:hover,
            [data-testid="stBaseButton-primary"]:focus,
            [data-testid="stBaseButton-primary"]:active {{
                background-color: var(--primary-color) !important;
                color: var(--button-text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Tabs with higher specificity */
            .stTabs [data-baseweb="tab-list"],
            .stTabs [data-baseweb="tab"],
            .stTabs [aria-selected="true"] {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Dataframes and tables with higher specificity */
            .stDataFrame,
            .stDataFrame > div,
            .stDataFrame > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Alerts and expanders with higher specificity */
            .stAlert,
            .stAlert > div,
            .stAlert > div > div,
            .stExpander,
            .stExpander > div,
            .stExpander > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Charts and graphs with higher specificity */
            .element-container,
            .element-container > div,
            .element-container > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Force all text to be visible with higher specificity */
            * {{
                color: var(--text-color) !important;
            }}

            /* Override specific Streamlit components with higher specificity */
            .stAlert,
            .stAlert > div,
            .stAlert > div > div,
            .stExpander,
            .stExpander > div,
            .stExpander > div > div,
            .stSelectbox,
            .stSelectbox > div,
            .stSelectbox > div > div,
            .stTextInput,
            .stTextInput > div,
            .stTextInput > div > div,
            .stNumberInput,
            .stNumberInput > div,
            .stNumberInput > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Override Streamlit's default theme with higher specificity */
            .stApp > header,
            .stApp > div,
            .stApp > div > div {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Ensure all text is visible with higher specificity */
            .stMarkdown,
            .stMarkdown > div,
            .stMarkdown > div > div {{
                color: var(--text-color) !important;
            }}

            /* Date input styling */
            [data-baseweb="input"] input,
            [data-baseweb="input"] input:focus,
            [data-baseweb="input"] input:hover,
            [data-baseweb="input"] input::placeholder,
            [data-testid="stDateInputField"],
            [data-testid="stDateInputField"]:focus,
            [data-testid="stDateInputField"]:hover,
            [data-testid="stDateInputField"]::placeholder,
            .stDateInput>div>div>input,
            .stDateInput>div>div>input:focus,
            .stDateInput>div>div>input:hover,
            .stDateInput>div>div>input::placeholder {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Date input calendar styling */
            [data-baseweb="calendar"] {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            [data-baseweb="calendar"] [role="gridcell"] {{
                color: var(--text-color) !important;
            }}

            [data-baseweb="calendar"] [role="gridcell"]:hover {{
                background-color: var(--hover-color) !important;
            }}

            [data-baseweb="calendar"] [aria-selected="true"] {{
                background-color: var(--primary-color) !important;
                color: var(--button-text-color) !important;
            }}

            /* Multi-select and tag styling */
            [data-baseweb="tag"],
            [data-baseweb="tag"] span,
            [data-baseweb="tag"] svg,
            [data-baseweb="tag"] path,
            .stMultiSelect>div>div>div,
            .stMultiSelect>div>div>div>div,
            .stMultiSelect>div>div>div>div>div,
            .stMultiSelect>div>div>div>div>div:hover,
            .stMultiSelect>div>div>div>div>div[aria-selected="true"],
            .stMultiSelect>div>div>div>div>div[aria-selected="true"]:hover {{
                background-color: var(--background-color-secondary) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Multi-select input field */
            .stMultiSelect>div>div>input,
            .stMultiSelect>div>div>input:focus,
            .stMultiSelect>div>div>input:hover,
            .stMultiSelect>div>div>input::placeholder {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Multi-select dropdown */
            [data-baseweb="popover"] {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Multi-select dropdown items */
            [data-baseweb="popover"] [role="option"],
            [data-baseweb="popover"] [role="option"]:hover,
            [data-baseweb="popover"] [role="option"][aria-selected="true"] {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Multi-select clear button */
            [data-baseweb="icon"][title="Clear all"],
            [data-baseweb="icon"][title="Clear all"] path {{
                color: var(--text-color) !important;
                fill: var(--text-color) !important;
            }}

            /* Multi-select dropdown arrow */
            [data-baseweb="icon"][title="open"],
            [data-baseweb="icon"][title="open"] path {{
                color: var(--text-color) !important;
                fill: var(--text-color) !important;
            }}

            /* Chart wrapper specific */
            .st-emotion-cache-0,
            .st-emotion-cache-b95f0i,
            .st-emotion-cache-4fcec4,
            .st-emotion-cache-q4kubj,
            .st-emotion-cache-8atqhb,
            .st-emotion-cache-2fmej3,
            .st-emotion-cache-yvmqfn,
            .stVegaLiteChart,
            .chart-wrapper,
            .chart-wrapper.fit-x,
            .chart-wrapper.fit-y,
            .chart-wrapper[role="graphics-document"],
            .chart-wrapper canvas.marks,
            .chart-wrapper .marks,
            .vega-embed,
            .vega-embed canvas,
            .vega-embed svg,
            .vega-embed .marks,
            .vega-embed .vega-bindings,
            .vega-embed .vega-bind-name,
            .vega-embed .vega-bind input,
            .vega-embed .vega-bind select,
            .vega-embed .vega-bind label,
            .vega-embed .vega-bindings,
            .vega-embed .vega-bindings * {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
            }}

            /* Chart container specific */
            .st-emotion-cache-0 {{
                background-color: var(--background-color) !important;
            }}

            .st-emotion-cache-b95f0i {{
                background-color: var(--background-color) !important;
            }}

            .st-emotion-cache-4fcec4 {{
                background-color: var(--background-color) !important;
            }}

            .st-emotion-cache-q4kubj {{
                background-color: var(--background-color) !important;
            }}

            .st-emotion-cache-8atqhb {{
                background-color: var(--background-color) !important;
            }}

            .st-emotion-cache-2fmej3 {{
                background-color: var(--background-color) !important;
            }}

            .st-emotion-cache-yvmqfn {{
                background-color: var(--background-color) !important;
            }}

            /* Chart wrapper specific */
            .chart-wrapper {{
                background-color: var(--background-color) !important;
            }}

            .chart-wrapper canvas {{
                background-color: var(--background-color) !important;
            }}

            .chart-wrapper .marks {{
                background-color: var(--background-color) !important;
            }}

            /* Vega embed specific */
            .vega-embed {{
                background-color: var(--background-color) !important;
            }}

            .vega-embed canvas {{
                background-color: var(--background-color) !important;
            }}

            .vega-embed .marks {{
                background-color: var(--background-color) !important;
            }}

            /* Chart axis and text */
            .vega-embed .vega-axes,
            .vega-embed .vega-axes text,
            .vega-embed .vega-title,
            .vega-embed .vega-title text,
            .vega-embed .vega-legend,
            .vega-embed .vega-legend text,
            .vega-embed .vega-legend-title,
            .vega-embed .vega-legend-title text {{
                color: var(--text-color) !important;
                fill: var(--text-color) !important;
                stroke: var(--text-color) !important;
            }}

            /* Chart grid lines */
            .vega-embed .vega-grid,
            .vega-embed .vega-grid line {{
                stroke: var(--border-color) !important;
            }}

            /* Chart tooltips */
            .vega-embed .vega-tooltip,
            .vega-embed .vega-tooltip table,
            .vega-embed .vega-tooltip tr,
            .vega-embed .vega-tooltip td,
            .vega-embed .vega-tooltip th {{
                background-color: var(--background-color) !important;
                color: var(--text-color) !important;
                border-color: var(--border-color) !important;
            }}

            /* Toggle button styling with increased specificity */
            [data-baseweb="checkbox"].st-ae [role="checkbox"],
            [data-baseweb="checkbox"].st-ae [role="checkbox"]:hover,
            [data-baseweb="checkbox"].st-ae [role="checkbox"][aria-checked="true"],
            [data-baseweb="checkbox"].st-d2 [role="checkbox"],
            [data-baseweb="checkbox"].st-d2 [role="checkbox"]:hover,
            [data-baseweb="checkbox"].st-d2 [role="checkbox"][aria-checked="true"],
            .st-ae [role="checkbox"],
            .st-ae [role="checkbox"]:hover,
            .st-ae [role="checkbox"][aria-checked="true"],
            .st-d2 [role="checkbox"],
            .st-d2 [role="checkbox"]:hover,
            .st-d2 [role="checkbox"][aria-checked="true"],
            .st-gc,
            .st-d2 {{
                background-color: var(--toggle-color) !important;
                border-color: var(--toggle-color) !important;
            }}

            [data-baseweb="checkbox"].st-ae [role="checkbox"]:hover,
            [data-baseweb="checkbox"].st-d2 [role="checkbox"]:hover,
            .st-ae [role="checkbox"]:hover,
            .st-d2 [role="checkbox"]:hover {{
                opacity: 0.9 !important;
            }}

            /* Persona tag styling */
            [data-baseweb="tag"],
            [data-baseweb="tag"] span,
            [data-baseweb="tag"] div {{
                background-color: var(--secondary-color) !important;
                color: white !important;
                border: none !important;
            }}

            /* Persona tag hover state */
            [data-baseweb="tag"]:hover {{
                opacity: 0.9 !important;
            }}

            /* Persona tag close button */
            [data-baseweb="tag"] button,
            [data-baseweb="tag"] button:hover {{
                background-color: transparent !important;
                color: white !important;
            }}

            /* Persona tag close button icon */
            [data-baseweb="tag"] button svg path {{
                fill: white !important;
            }}

            /* Toggle background styling using Streamlit's specific classes */
            .st-b0.st-c2.st-ch.st-ci.st-cj.st-ck.st-ag.st-cl.st-cm.st-ax.st-au.st-av.st-cn.st-co.st-cp.st-cq.st-cr,
            .st-cs.st-ct.st-cu.st-cv.st-cw.st-cx.st-bi.st-cy.st-cz.st-gp.st-d1 {{
                background-color: var(--secondary-color) !important;
            }}

            /* Toggle switch thumb/handle styling using Streamlit's specific classes */
            .st-b1.st-b2.st-b3.st-b4.st-b5.st-b6.st-b7.st-b8.st-b9.st-gc.st-bb {{
                background-color: #9ca3af !important;  /* gray-400 */
            }}

            /* Toggle switch thumb/handle hover state */
            .st-b1.st-b2.st-b3.st-b4.st-b5.st-b6.st-b7.st-b8.st-b9.st-gc.st-bb:hover {{
                background-color: #6b7280 !important;  /* gray-500 */
            }}

            /* Tab highlight styling using Streamlit's specific classes */
            [data-baseweb="tab-highlight"].st-e0.st-bi.st-e1.st-e2.st-e3.st-e4.st-fp.st-e6,
            div[data-baseweb="tab-highlight"][aria-hidden="true"][role="presentation"] {{
                background-color: var(--secondary-color) !important;
            }}

            /* Override Streamlit's default red color */
            .stButton>button,
            .stButton>button:focus,
            .stButton>button:hover,
            .stButton>button:active,
            .stDownloadButton>button,
            .stDownloadButton>button:focus,
            .stDownloadButton>button:hover,
            .stDownloadButton>button:active,
            .element-container .stButton>button,
            .element-container .stButton>button:focus,
            .element-container .stButton>button:hover,
            .element-container .stButton>button:active,
            .element-container .stDownloadButton>button,
            .element-container .stDownloadButton>button:focus,
            .element-container .stDownloadButton>button:hover,
            .element-container .stDownloadButton>button:active,
            [data-testid="stBaseButton"],
            [data-testid="stBaseButton"]:focus,
            [data-testid="stBaseButton"]:hover,
            [data-testid="stBaseButton"]:active,
            [data-testid="stBaseButton-primary"],
            [data-testid="stBaseButton-primary"]:focus,
            [data-testid="stBaseButton-primary"]:hover,
            [data-testid="stBaseButton-primary"]:active,
            [data-testid="stBaseButton-secondary"],
            [data-testid="stBaseButton-secondary"]:focus,
            [data-testid="stBaseButton-secondary"]:hover,
            [data-testid="stBaseButton-secondary"]:active,
            .st-emotion-cache-zaw6nw,
            .st-emotion-cache-zaw6nw:focus,
            .st-emotion-cache-zaw6nw:hover,
            .st-emotion-cache-zaw6nw:active {{
                background-color: #3b82f6 !important;
                border-color: #3b82f6 !important;
            }}

            /* Override Streamlit's default red color for text */
            .stButton>button p,
            .stDownloadButton>button p,
            .element-container .stButton>button p,
            .element-container .stDownloadButton>button p,
            .st-emotion-cache-zaw6nw p,
            .st-emotion-cache-zaw6nw .st-emotion-cache-p7i6r9 p,
            .st-emotion-cache-zaw6nw .st-emotion-cache-p7i6r9,
            [data-testid="stBaseButton"] p,
            [data-testid="stBaseButton"] .st-emotion-cache-p7i6r9 p,
            [data-testid="stBaseButton"] .st-emotion-cache-p7i6r9,
            [data-testid="stBaseButton-secondary"] p,
            [data-testid="stBaseButton-secondary"] .st-emotion-cache-p7i6r9 p,
            [data-testid="stBaseButton-secondary"] .st-emotion-cache-p7i6r9 {{
                color: var(--button-text-color) !important;
            }}

            /* Override Streamlit's default red color for hover states */
            .stButton>button:hover,
            .stDownloadButton>button:hover,
            .element-container .stButton>button:hover,
            .element-container .stDownloadButton>button:hover,
            [data-testid="stBaseButton"]:hover,
            [data-testid="stBaseButton-primary"]:hover,
            [data-testid="stBaseButton-secondary"]:hover,
            .st-emotion-cache-zaw6nw:hover {{
                background-color: #2563eb !important;  /* Slightly darker blue for hover */
                border-color: #2563eb !important;
            }}

            /* Override Streamlit's default red color for focus states */
            .stButton>button:focus,
            .stDownloadButton>button:focus,
            .element-container .stButton>button:focus,
            .element-container .stDownloadButton>button:focus,
            [data-testid="stBaseButton"]:focus,
            [data-testid="stBaseButton-primary"]:focus,
            [data-testid="stBaseButton-secondary"]:focus,
            .st-emotion-cache-zaw6nw:focus {{
                background-color: #2563eb !important;
                border-color: #2563eb !important;
                box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3) !important;
            }}

            /* Override Streamlit's default red color for active states */
            .stButton>button:active,
            .stDownloadButton>button:active,
            .element-container .stButton>button:active,
            .element-container .stDownloadButton>button:active,
            [data-testid="stBaseButton"]:active,
            [data-testid="stBaseButton-primary"]:active,
            [data-testid="stBaseButton-secondary"]:active,
            .st-emotion-cache-zaw6nw:active {{
                background-color: #1d4ed8 !important;  /* Even darker blue for active */
                border-color: #1d4ed8 !important;
            }}

            /* Tooltip styles are now in styles.css for better theme support */

            /* KPI badge styling */
            .kpi-badge {{
                background-color: var(--primary-color) !important;
                color: white !important;
                padding: 0.25rem 0.75rem !important;
                border-radius: 9999px !important;
                font-size: 0.75rem !important;
                font-weight: 500 !important;
            }}
        </style>
    """
    
    logger.debug("Applying CSS styles")
    st.markdown(css, unsafe_allow_html=True)
    
    # Add JavaScript to set data-theme and accessibility attributes on body
    accessibility_attrs = []
    if st.session_state.theme.get('high_contrast'):
        accessibility_attrs.append('high-contrast')
    if st.session_state.theme.get('reduced_motion'):
        accessibility_attrs.append('reduced-motion')
    
    accessibility_attr = ' '.join(accessibility_attrs) if accessibility_attrs else ''
    
    st.markdown(f"""
        <script>
            document.addEventListener('DOMContentLoaded', function() {{
                document.body.setAttribute('data-theme', '{theme_mode}');
                document.documentElement.setAttribute('data-theme', '{theme_mode}');
                if ('{accessibility_attr}') {{
                    document.body.setAttribute('data-accessibility', '{accessibility_attr}');
                    document.documentElement.setAttribute('data-accessibility', '{accessibility_attr}');
                }}
            }});
            // Also set immediately in case DOM is already loaded
            document.body.setAttribute('data-theme', '{theme_mode}');
            document.documentElement.setAttribute('data-theme', '{theme_mode}');
            if ('{accessibility_attr}') {{
                document.body.setAttribute('data-accessibility', '{accessibility_attr}');
                document.documentElement.setAttribute('data-accessibility', '{accessibility_attr}');
            }}
        </script>
    """, unsafe_allow_html=True)

def toggle_theme():
    """Toggle between light and dark mode."""
    logger.debug("Toggling theme...")
    debug_session_state()
    
    # Ensure theme state exists
    if 'theme' not in st.session_state:
        logger.debug("Theme not found in session state, initializing")
        initialize_theme()
    
    # Get current state from theme_toggle if it exists, otherwise use theme state
    current_state = st.session_state.get('theme_toggle', st.session_state.theme['dark_mode'])
    logger.debug(f"Current theme_toggle state: {current_state}")
    
    # Update theme state
    st.session_state.theme['dark_mode'] = current_state
    st.session_state.theme['last_updated'] = datetime.now()
    
    logger.debug(f"New theme state: {st.session_state.theme}")
    
    # Re-apply the theme
    apply_theme()

# Accessibility functions kept for programmatic use but toggles removed from UI

def render_theme_toggle():
    """Render the theme toggle in the sidebar."""
    logger.debug("Rendering theme toggle...")
    debug_session_state()
    
    with st.sidebar:
        # Ensure theme state exists
        if 'theme' not in st.session_state:
            logger.debug("Theme not found in session state, initializing")
            initialize_theme()
        
        # Theme section
        st.markdown("### 🎨 Theme")
        
        # Initialize theme toggle if it doesn't exist
        if 'theme_toggle' not in st.session_state:
            st.session_state.theme_toggle = st.session_state.theme['dark_mode']
        
        # Create the dark mode toggle only
        st.toggle(
            "🌙 Dark Mode",
            value=st.session_state.theme['dark_mode'],
            key="theme_toggle",
            help="Toggle between light and dark mode",
            on_change=toggle_theme
        ) 