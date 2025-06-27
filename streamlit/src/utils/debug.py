"""
Debug utility functions for Streamlit dashboard components.
"""

import streamlit as st
import pandas as pd
from typing import Any, Dict, Optional
from pathlib import Path

def display_debug_info(
    sql_file_path: str,
    params: Dict[str, Any],
    results: pd.DataFrame,
    query_name: Optional[str] = None,
    show_raw_sql: bool = True,
    show_params: bool = True,
    show_results: bool = True,
    show_df_preview: bool = True,
    max_rows: int = 5
) -> None:
    """Display standardized debug information for a query.
    
    Args:
        sql_file_path: Path to the SQL file relative to src/sql/
        params: Dictionary of query parameters
        results: DataFrame containing query results
        query_name: Optional name for the query (used in headers)
        show_raw_sql: Whether to show the raw SQL query
        show_params: Whether to show the query parameters
        show_results: Whether to show the raw results
        show_df_preview: Whether to show a DataFrame preview
        max_rows: Maximum number of rows to show in preview
    """
    if not st.session_state.get('debug_mode', False):
        return
        
    query_name = query_name or sql_file_path.split('/')[-1].replace('.sql', '')
    
    st.markdown(f"### {query_name} Debug Information")
    
    if show_raw_sql:
        st.markdown("#### SQL Query")
        try:
            with open(Path(__file__).parent.parent / "sql" / sql_file_path) as f:
                st.code(f.read(), language="sql")
        except FileNotFoundError:
            st.error(f"SQL file not found: {sql_file_path}")
    
    if show_params:
        st.markdown("#### Query Parameters")
        st.json(params)
    
    if show_results and not results.empty:
        st.markdown("#### Query Results")
        st.json(results.to_dict(orient='records'))
    
    if show_df_preview and not results.empty:
        st.markdown("#### DataFrame Preview")
        st.dataframe(results.head(max_rows))

def initialize_debug_mode() -> None:
    """Initialize debug mode in session state if not already set."""
    if 'debug_mode' not in st.session_state:
        st.session_state.debug_mode = False

def render_global_debug_toggle() -> None:
    """Render the global debug mode toggle in the sidebar."""
    initialize_debug_mode()
    with st.sidebar:
        st.toggle(
            "Debug Mode",
            key="debug_mode",
            help="Toggle debug information display across all dashboard components"
        )

def read_sql_file(file_path: str) -> str:
    """Read and return the contents of a SQL file.
    
    Args:
        file_path: Path to the SQL file relative to src/sql/
        
    Returns:
        str: Contents of the SQL file
    """
    import os
    full_path = os.path.join("src", "sql", file_path)
    with open(full_path, "r") as f:
        return f.read() 