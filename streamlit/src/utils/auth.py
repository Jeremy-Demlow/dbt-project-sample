"""
Utilities for authenticating to Snowflake REST APIs.
"""
import streamlit as st
# import jwt # Removed jwt import
from cryptography.hazmat.primitives import serialization # This might be unused now
from cryptography.hazmat.backends import default_backend # This might be unused now
import datetime # This might be unused now
import time # This might be unused now
import snowflake.connector

# Removed _get_qualified_names function as it was JWT specific
# def _get_qualified_names(account_locator_raw: str, user_raw: str, public_key_fp_raw: str):
#     ...

def get_snowflake_jwt() -> str | None:
    """
    Obtains a session token using username/password authentication.
    Username/password auth reads credentials from st.secrets.snowflake.
    Returns the session token string or None if an error occurs.
    """
    # Key-Pair authentication block has been removed as per request to remove JWT references.
    
    # Attempt Username/Password authentication
    snow_creds = st.secrets.get("snowflake")
    if snow_creds and snow_creds.get("user") and snow_creds.get("password") and snow_creds.get("account"):
        try:
            conn = snowflake.connector.connect(
                user=snow_creds.get("user"),
                password=snow_creds.get("password"),
                account=snow_creds.get("account"),
                warehouse=snow_creds.get("warehouse"), # Optional but good to include if present
                database=snow_creds.get("database"),   # Optional
                schema=snow_creds.get("schema"),       # Optional
                role=snow_creds.get("role"),           # Optional
                session_parameters={
                    'QUERY_TAG': 'CortexAnalystStreamlitAppAuth',
                }
            )
            # The session token is what we need for API calls.
            # Note: The Snowflake Connector's session token might have a different lifetime.
            # Typically 4 hours for connector.
            session_token = conn.sfqid # sfqid is often the session ID, token is often in master_token or session_token
            
            # snowflake-connector-python versions >= 2.7.0 provide session_token directly
            # For older versions, it might be master_token or require more complex extraction
            # Let's try to get it from standard places.
            # The REST API needs the raw token, not the "Snowflake" prefixed one if using SnowCDK generated clients.
            # However, for direct REST calls to /api/v2/cortex/analyst, the session token obtained from connector works.
            
            # Check common attributes for the session token
            if hasattr(conn, 'token'): # For some connection methods or future versions
                token_to_use = conn.token
            elif hasattr(conn, 'session_token'): # Standard for recent connectors
                token_to_use = conn.session_token
            elif hasattr(conn, '_rest') and hasattr(conn._rest, 'token'): # Internal but sometimes accessed
                 token_to_use = conn._rest.token
            else:
                # Fallback or less ideal: try to force a query to ensure token is populated if lazy.
                # However, the token should be available after connect().
                # For some auth methods (like SSO), token might be populated later.
                # For user/pass it should be immediate.
                # If sfqid is the only easily accessible identifier and it's not the token, this will fail.
                # The Snowflake documentation indicates that the session token used by REST APIs
                # is the one you get from the connector.
                st.error("Authentication Error (User/Pass): Could not retrieve session token from Snowflake connection. Connector version might matter.")
                conn.close()
                return None

            if token_to_use and token_to_use.startswith("Snowflake "): # Some contexts add this prefix
                 token_to_use = token_to_use.split(" ")[1]


            conn.close()
            if token_to_use:
                # st.success("Successfully authenticated using username/password.")
                return token_to_use
            else:
                st.error("Authentication Error (User/Pass): Retrieved token was empty.")
                return None

        except snowflake.connector.errors.DatabaseError as e:
            # Handle Snowflake specific connection errors (e.g., wrong credentials, account)
            st.error(f"Snowflake Connection Error (User/Pass): {e}")
            return None
        except Exception as e:
            st.error(f"Authentication Error (User/Pass): Could not connect to Snowflake or get session token. Details: {e}")
            return None
            
    # If username/password method failed or was not configured
    st.error("Authentication Error: Username/Password (snowflake) credentials are not fully configured or valid in secrets.toml.")
    return None

def get_snowflake_api_base_url() -> str | None:
    """
    Constructs the base URL for Snowflake REST API calls.
    Reads account_url_identifier from st.secrets.snowflake_api_auth or st.secrets.snowflake.
    Returns the base URL string or None if an error occurs.
    """
    account_url_id = None
    
    # Try snowflake_api_auth first
    api_creds = st.secrets.get("snowflake_api_auth")
    if api_creds and api_creds.get("account_url_identifier"):
        account_url_id = api_creds.get("account_url_identifier")
    
    # If not found, try snowflake section
    if not account_url_id:
        snow_creds = st.secrets.get("snowflake")
        if snow_creds and snow_creds.get("account_url_identifier"): # Expect explicit field
            account_url_id = snow_creds.get("account_url_identifier")
        elif snow_creds and snow_creds.get("account"): # Fallback: try to derive from 'account'
            # Basic derivation: replace underscores with hyphens and lowercase.
            # This might not cover all org/account formats.
            # Example: my_org_my_account -> my-org-my-account
            # Example: xy12345 -> xy12345
            # The account_url_identifier is typically <organization_name>-<account_name> or <account_locator>
            # If snow_creds.get("account") is like "SFSEEUROPE-DEMO_AROSS", it might need processing.
            # For "SFSEEUROPE-DEMO_AROSS", the url part is often "sfseeurope-demo_aross" or "sfseeurope.demo_aross" (if old format)
            # Or it could be an account locator like "xy12345".
            # The safest is to require "account_url_identifier" explicitly.
            # For now, let's try a simple conversion, but warn if it's a guess.
            raw_account = snow_creds.get("account")
            if '.' in raw_account: # Likely an old format like account_locator.region.cloud
                account_url_id = raw_account.split('.')[0].lower() # Best guess
            else: # New format org-account or locator
                 # If it contains '_', it's likely an account NAME that needs to map to a locator or url id
                 # If it's like SFSEUROPE-DEMO_AROSS, it might be the account_name part of an org-account_name
                 # The user provided account = "SFSEEUROPE-DEMO_AROSS"
                 # The account_url_identifier is typically the part before '.snowflakecomputing.com'
                 # e.g. if url is myorg-myaccount.snowflakecomputing.com, identifier is myorg-myaccount.
                 # If account is "XY12345", identifier is "xy12345".
                 # The provided "SFSEEUROPE-DEMO_AROSS" looks like an account name, not an account locator or full URL identifier.
                 # It's better to require `account_url_identifier` in `st.secrets.snowflake` too.
                 st.warning("Could not find 'account_url_identifier' in 'snowflake' secrets. "
                            "Please add 'account_url_identifier' to your 'secrets.toml' under the '[snowflake]' section. "
                            "It's the part of your Snowflake URL like 'yourorg-youraccount' or 'xy12345'.")
                 # Returning None here to force the user to add it.

    if not account_url_id:
        st.error(
            "Configuration Error: 'account_url_identifier' not found in "
            "'snowflake_api_auth' or 'snowflake' sections in secrets.toml."
        )
        return None
        
    return f"https://{account_url_id}.snowflakecomputing.com" 