import streamlit as st
import os
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    try:
        # Try to get the active session, which exists when running in Snowflake
        session = get_active_session()
        return session
    except:
        # Fallback to st.connection for local development or other environments
        return st.connection("snowflake")

# Helper function to load queries from .sql files
def load_query(query_file_name: str) -> str:
    """Loads a SQL query from the streamlit_app/queries folder or its subdirectories."""
    # Get the directory containing the current script (utils.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the query file, assuming 'queries' is a sibling to utils.py
    # and query_file_name might include subdirectories relative to 'queries'
    # e.g. query_file_name = "some_query.sql" or "tab_subfolder/some_query.sql"
    query_path = os.path.join(base_dir, "queries", query_file_name)
    try:
        with open(query_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        st.error(f"Query file not found: {query_path}. Ensure the file exists at this path relative to the 'streamlit_app' directory.")
        return "" 
    except Exception as e:
        st.error(f"Error loading query from {query_path}: {e}")
        return "" 

def execute_query(connection, query_string: str) -> pd.DataFrame:
    """Executes a SQL query using the appropriate method based on the connection type."""
    if not query_string:
        st.warning("Query string is empty. Returning empty DataFrame.")
        return pd.DataFrame()
    try:
        if hasattr(connection, 'query'): # Streamlit native connection
            return connection.query(query_string)
        else: # Snowpark session
            return connection.sql(query_string).to_pandas()
    except Exception as e:
        st.error(f"Error executing query: {e}\nQuery: {query_string[:200]}..." ) # Show partial query on error
        return pd.DataFrame() # Return empty DataFrame on error

def handle_error(message: str, exception: Exception = None):
    """Displays an error message in Streamlit.

    Args:
        message (str): The error message to display.
        exception (Exception, optional): The exception object, if any. Defaults to None.
    """
    if exception:
        st.error(f"{message}: {exception}")
    else:
        st.error(message) 