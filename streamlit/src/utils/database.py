"""
Snowflake database connection and query execution utilities.
Handles connection pooling, query execution, and result caching.
"""

from typing import Any, Dict, List, Optional, Union
import os
import snowflake.connector
from snowflake.connector import DictCursor
import streamlit as st
import pandas as pd

# Attempt to import Snowpark Session for type checking in SiS environment detection
try:
    from snowflake.snowpark.session import Session as SnowparkSession
    from snowflake.snowpark.context import get_active_session as get_snowpark_active_session
except ImportError:
    SnowparkSession = None # Snowpark not installed, or not in the Python path
    get_snowpark_active_session = None

print(f"DEBUG: SnowparkSession is None after import: {SnowparkSession is None}") # DEBUG PRINT

class SnowflakeConnection:
    """Manages Snowflake connection and query execution, adapting to SiS or local."""
    
    def __init__(self):
        """Initialize connection, detecting if running in Streamlit-in-Snowflake or locally."""
        self._snowpark_session = None
        self._local_raw_connection = None
        self._is_sis = False # Assume local unless proven SiS

        try:
            conn_obj = st.connection("snowflake")
            print(f"DEBUG: conn_obj type: {type(conn_obj)}")

            connection_successful = False # Flag to track if any method succeeds

            # Pattern 1: conn_obj.session is a callable method (e.g., .session() from Snowflake docs)
            if not connection_successful and SnowparkSession and hasattr(conn_obj, 'session') and callable(getattr(conn_obj, 'session')):
                print("DEBUG: Attempting Pattern 1: conn_obj.session() as a method.")
                try:
                    potential_session = conn_obj.session() # Call it as a method
                    print(f"DEBUG: conn_obj.session() returned type: {type(potential_session)}")
                    if potential_session is not None and isinstance(potential_session, SnowparkSession):
                        self._snowpark_session = potential_session
                        self._is_sis = True
                        self._snowpark_session.sql("SELECT 1 AS test_col").collect() # Test SiS connection
                        connection_successful = True
                        print("DEBUG: Success via Pattern 1: conn_obj.session()")
                    else:
                        print("DEBUG: Pattern 1: conn_obj.session() did not return a valid SnowparkSession instance or was None.")
                except Exception as call_e:
                    print(f"DEBUG: Pattern 1: Error calling conn_obj.session(): {str(call_e)}")

            # Pattern 2: conn_obj.session is an attribute that is a SnowparkSession
            if not connection_successful and SnowparkSession and hasattr(conn_obj, 'session') and \
               not callable(getattr(conn_obj, 'session')) and \
               getattr(conn_obj, 'session') is not None and isinstance(getattr(conn_obj, 'session'), SnowparkSession):
                print("DEBUG: Attempting Pattern 2: conn_obj.session as an attribute.")
                self._snowpark_session = getattr(conn_obj, 'session')
                self._is_sis = True
                self._snowpark_session.sql("SELECT 1 AS test_col").collect() # Test SiS connection
                connection_successful = True
                print("DEBUG: Success via Pattern 2: conn_obj.session attribute.")

            # Pattern 3: conn_obj itself is a SnowparkSession
            if not connection_successful and SnowparkSession and isinstance(conn_obj, SnowparkSession):
                print("DEBUG: Attempting Pattern 3: conn_obj directly as SnowparkSession.")
                self._snowpark_session = conn_obj
                self._is_sis = True
                self._snowpark_session.sql("SELECT 1 AS test_col").collect() # Test SiS connection
                connection_successful = True
                print("DEBUG: Success via Pattern 3: conn_obj directly.")

            # Pattern 4: Local raw Snowflake connection (SnowflakeConnection)
            if not connection_successful and hasattr(conn_obj, '_instance') and conn_obj._instance is not None and \
                 isinstance(conn_obj._instance, snowflake.connector.SnowflakeConnection):
                print("DEBUG: Attempting Pattern 4: local raw connection via conn_obj._instance.")
                self._local_raw_connection = conn_obj._instance
                # self._is_sis remains False (default for local)
                with self._local_raw_connection.cursor() as cur_test:
                    cur_test.execute("SELECT 1 AS test_col")
                connection_successful = True # A connection (local) was made
                print("DEBUG: Success via Pattern 4: local raw connection.")
            
            # Check if any connection method succeeded
            if not self._snowpark_session and not self._local_raw_connection:
                print(f"DEBUG: All connection patterns failed. conn_obj attributes: {dir(conn_obj)}") # DEBUG PRINT
                raise Exception("st.connection('snowflake') returned an unrecognized object configuration or failed all known patterns.")

        except AttributeError as e:
            if "module 'streamlit' has no attribute 'connection'" in str(e):
                # ----- Attempt 2: Fallback for SiS with older Streamlit (st.connection missing) -----
                if SnowparkSession and get_snowpark_active_session:
                    try:
                        # st.info("Attempting SiS fallback: get_active_session()") # Optional debug info
                        self._snowpark_session = get_snowpark_active_session()
                        if self._snowpark_session and isinstance(self._snowpark_session, SnowparkSession):
                            self._is_sis = True
                            self._snowpark_session.sql("SELECT 1 AS test_col").collect() # Test SiS connection
                        else:
                            self._snowpark_session = None # Ensure it's reset if not a valid session
                            raise Exception("SiS fallback: get_active_session() did not return a valid Snowpark session.")
                    except Exception as fallback_e:
                        self._snowpark_session = None # Ensure reset on error
                        raise Exception(f"SiS fallback using get_active_session() failed: {fallback_e}")
                else:
                    # st.connection missing and no Snowpark fallback possible (e.g., local old Streamlit without Snowpark)
                    raise Exception("st.connection is unavailable, and Snowpark context (get_active_session) is also not available. Update Streamlit or check environment.")
            else:
                # Different AttributeError, re-raise it as it's unexpected
                raise
        except Exception as e:
            # Catch any other unexpected errors during connection setup
            raise Exception(f"An unexpected error occurred during Snowflake connection setup: {e}")

        # Final validation: ensure one connection method succeeded
        if not self._snowpark_session and not self._local_raw_connection:
            # This state should ideally be caught by specific errors above, but as a safeguard:
            raise Exception("Failed to establish Snowflake connection through any available method. Please check logs for specific errors.")

    def _read_sql_file(self, sql_path: str) -> str:
        """Read SQL query from a file.
        
        Args:
            sql_path: Path to the SQL file relative to the main 'sql' directory
                      (e.g., "my_query.sql" or "subdir/my_query.sql").
            
        Returns:
            str: SQL query string
        """
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(utils_dir)
        full_path = os.path.join(project_root, "sql", sql_path)
        try:
            with open(full_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            st.error(f"SQL file not found at path: {full_path}")
            raise
    
    def _substitute_params(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Substitute named parameters in the query string. WARNING: SQL INJECTION RISK."""
        if params:
            for key, value in params.items():
                if isinstance(value, list):
                    if not value:
                        formatted_value = "''"
                    else:
                        # This specific list formatting might need review based on SQL usage (e.g., for IN clauses)
                        # Original code created a single string like "'val1,val2,val3'".
                        # For an IN clause, one might need "('val1', 'val2')"
                        # Replicating previous logic, ensuring values are strings and single quotes within them are escaped.
                        escaped_list_items = [str(v).replace("'", "''") for v in value]
                        formatted_value = f"'{','.join(escaped_list_items)}'"
                elif isinstance(value, str):
                    escaped_value = value.replace("'", "''") # Escape single quotes
                    formatted_value = f"'{escaped_value}'"
                elif isinstance(value, (int, float)):
                    formatted_value = str(value)
                elif value is None:
                    formatted_value = "NULL"
                else:
                    # Fallback: convert to string and escape single quotes
                    escaped_value = str(value).replace("'", "''")
                    formatted_value = f"'{escaped_value}'"
                
                query = query.replace(f":{key}", formatted_value)
        return query

    @st.cache_data(ttl=300)
    def execute_query(
        _self, # _self refers to the instance of SnowflakeConnection
        query_or_path: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries.
        
        Args:
            query_or_path: SQL query string or path to SQL file relative to src/sql/
            params: Dictionary of parameter values
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            Exception: If query execution fails
        """
        
        final_query: str
        if query_or_path.endswith('.sql'):
            final_query = _self._read_sql_file(query_or_path)
        else:
            final_query = query_or_path
        
        # Apply the custom parameter substitution.
        # WARNING: This is a SQL injection risk and should be replaced with proper parameterized queries.
        final_query = _self._substitute_params(final_query, params)
            
        try:
            if _self._is_sis:
                if not _self._snowpark_session:
                    raise Exception("Streamlit-in-Snowflake mode, but Snowpark session is not available.")
                # st.write(f"[SiS] Executing: {final_query}")
                snowpark_rows = _self._snowpark_session.sql(final_query).collect()
                return [row.as_dict() for row in snowpark_rows]
            else: # Local execution
                if not _self._local_raw_connection:
                    raise Exception("Local mode, but raw Snowflake connection is not available.")
                # st.write(f"[Local] Executing: {final_query}")
                with _self._local_raw_connection.cursor(DictCursor) as cur: # Use positional DictCursor
                    # Query tag is good practice
                    try: cur.execute("ALTER SESSION SET QUERY_TAG = 'streamlit_app_local'")
                    except Exception: pass # Best effort
                    
                    cur.execute(final_query) # Params are already substituted into final_query
                    results = cur.fetchall()
                    return results
                    
        except Exception as e:
            st.error(f"Error executing query ({'SiS' if _self._is_sis else 'Local'}): {str(e)}\\nQuery (potentially with substituted params): {final_query}")
            raise

# Global instance for other functions to use, initialized when module is imported.
# This was the original pattern. Consider if `run_query` should take an instance.
snowflake_conn = SnowflakeConnection()

def run_query(query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.
    
    Args:
        query: SQL query string or path to .sql file
        params: Optional dictionary of query parameters
        
    Returns:
        pandas DataFrame with query results
    """
    # The original run_query created a new SnowflakeConnection() instance each time.
    # This is inefficient. Using the global instance is better.
    # If snowflake_conn failed to initialize, this will use a broken object.
    # Initialization errors in snowflake_conn should be fatal or clearly indicated.

    #st.write(f"run_query called with: {query}, params: {params}")
    results = snowflake_conn.execute_query(query, params)
    df = pd.DataFrame(results)
    #st.write("Query results DataFrame info:")
    #st.write(df.info() if not df.empty else "DataFrame is empty.")
    #st.write("Query results columns:", df.columns.tolist())
    return df 