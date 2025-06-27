import sys
import os
sys.path.append(os.path.dirname(__file__))

import streamlit as st
import os
import time
import json
import requests
import pandas as pd # Ensure pandas is imported
from datetime import datetime, timedelta
from utils.utils import get_snowflake_connection, execute_query # Assuming load_query is not needed for this specific flow, added execute_query
import toml # Added for parsing config.toml

"""
Cortex Analyst Component with Robust Fragment Management

This module implements a high-performance Cortex Analyst interface using Streamlit's fragment system
with proper fragment lifecycle management to prevent DOM conflicts and fragment removal errors.

FRAGMENT LIFECYCLE MANAGEMENT:
1. Stable Fragment IDs: Uses consistent fragment IDs that don't change across reruns
2. Fragment State Isolation: Prevents fragment conflicts during full-app reruns
3. Proper Cleanup: Handles fragment removal and recreation gracefully
4. Error Recovery: Automatically recovers from fragment lifecycle issues

ROBUST ARCHITECTURE:
- Single primary fragment with stable ID management
- Proper session state synchronization
- Fragment-safe API call handling
- Graceful degradation when fragments are removed
"""

# --- Environment Detection ---
_IS_SNOWFLAKE_ENVIRONMENT = False
try:
    import _snowflake
    _IS_SNOWFLAKE_ENVIRONMENT = True
except ImportError:
    pass

# Additional Snowflake environment detection
def detect_snowflake_environment():
    """Enhanced Snowflake environment detection"""
    if _IS_SNOWFLAKE_ENVIRONMENT:
        return True
    
    # Check for Snowflake-specific environment variables or modules
    try:
        # Check if we're running in Snowflake's managed environment
        if 'SNOWFLAKE_ACCOUNT' in os.environ:
            return True
        if hasattr(st, 'connection') and 'snowflake' in str(st.connection):
            return True
    except:
        pass
    
    return False

_IS_SIS_ENVIRONMENT = detect_snowflake_environment()

# --- Constants ---
SEMANTIC_MODEL_PATH = "@DBT_CORTEX_LLMS.SEMANTIC_MODELS.YAML_STAGE/semantic_model.yaml"
API_TIMEOUT_SECONDS = 120
SNOWFLAKE_CORTEX_ANALYST_API_PATH = "/api/v2/cortex/analyst/message"

# Import run_query from utils.database
from utils.database import run_query

# Import accessibility utilities
from utils.accessibility import (
    announce_to_screen_reader, 
    create_skip_link, 
    create_loading_announcement,
    create_error_announcement,
    create_success_announcement,
    get_javascript_utilities,
    create_keyboard_help_section
)

# Import Dynamic Chart Intelligence components
from utils.chart_intelligence import QueryResultAnalyzer, VisualizationRuleEngine
from utils.chart_factory import ChartSelector 

def render_cortex_analyst_tab(filters: dict, debug_mode: bool = False):
    """Render the 'Ask Your Data' tab with robust fragment management"""
    
    # Add accessibility JavaScript utilities
    st.markdown(get_javascript_utilities(), unsafe_allow_html=True)
    
    # Initialize session state with stable management
    initialize_session_state_robust()
    
    # Header section
    st.markdown(f'''
        <section id="sample-questions" class="card-section" aria-labelledby="sample-questions-heading">
            <h2 id="sample-questions-heading" class="heading-secondary">🗣️ Ask Your Data</h2>
        </section>
    ''', unsafe_allow_html=True)
    
    # Use single robust fragment approach
    cortex_analyst_main_fragment()
    
    # Debug information
    if debug_mode or st.session_state.get("debug_mode"):
        render_debug_information()

def initialize_session_state_robust():
    """Initialize session state with robust fragment management"""
    
    # Core state variables
    if 'user_question' not in st.session_state:
        st.session_state.user_question = ""
    if 'show_more_samples' not in st.session_state:
        st.session_state.show_more_samples = False
    if 'question_submitted' not in st.session_state:
        st.session_state.question_submitted = False
    if 'cortex_analyst_response' not in st.session_state:
        st.session_state.cortex_analyst_response = None
    
    # Fragment management - use stable counter instead of incrementing keys
    if 'fragment_initialized' not in st.session_state:
        st.session_state.fragment_initialized = True
        st.session_state.fragment_counter = 0
    
    # Track fragment state to prevent conflicts
    if 'fragment_active' not in st.session_state:
        st.session_state.fragment_active = False

@st.fragment
def cortex_analyst_main_fragment():
    """
    Main fragment for Cortex Analyst with robust lifecycle management.
    
    This fragment handles all interactions in a single, stable fragment to prevent
    fragment removal conflicts and DOM manipulation errors.
    """
    
    try:
        # Mark fragment as active
        st.session_state.fragment_active = True
        
        # Render all components within single fragment
        render_sample_questions_robust()
        render_question_input_robust()
        
        # Handle API calls within fragment context
        handle_api_calls_robust()
        
        # Display response if available
        if st.session_state.cortex_analyst_response:
            render_response_robust()
            
    except Exception as e:
        # Handle fragment lifecycle errors gracefully
        if "does not exist anymore" in str(e) or "fragment" in str(e).lower():
            st.warning("🔄 Refreshing interface... Please try your action again.")
            st.session_state.fragment_active = False
            # Trigger a full rerun to recreate fragment
            st.rerun()
        else:
            st.error(f"An error occurred: {str(e)}")
            # Log the error for debugging
            if st.session_state.get("debug_mode"):
                st.exception(e)

def render_sample_questions_robust():
    """Render sample questions with stable keys"""
    
    primary_questions = [
        "Can I see a count of customers and their average sentiment score, grouped by predicted churn risk level?",
        "Can you list my top 20 high LTV customers with 'High' churn_risk and a significantly negative sentiment trend, showing key persona signals?"
    ]
    
    secondary_questions = [
        "Can you identify products where I have an average rating below 3.0 or average review sentiment below -0.1, highlighting potential problem areas?",
        "Can you analyze my critical and high priority support tickets, grouped by derived customer persona and ticket category?",
        "Show me customer interaction trends over the last 90 days, segmented by communication channel",
        "What are the most common themes in negative product reviews for our highest value customers?"
    ]

    def populate_question_safe(question_text):
        """Safely populate question without causing fragment conflicts"""
        st.session_state.user_question = question_text
        # Use fragment-scoped rerun to avoid full app rerun
        st.rerun(scope="fragment")

    # Sample questions section
    st.markdown('''
        <h3 class="heading-tertiary">Try these sample questions:</h3>
        <div id="sample-questions-help" class="sr-only">
            Use these sample questions to get started. Click any question to populate the input field.
        </div>
    ''', unsafe_allow_html=True)
    
    # Primary sample questions with stable keys
    col1, col2 = st.columns(2, gap="medium")
    
    with col1:
        if st.button(
            primary_questions[0], 
            key="sample_primary_1", 
            help="Click to use this sample question",
            use_container_width=True
        ):
            populate_question_safe(primary_questions[0])
    
    with col2:
        if st.button(
            primary_questions[1], 
            key="sample_primary_2", 
            help="Click to use this sample question",
            use_container_width=True
        ):
            populate_question_safe(primary_questions[1])
    
    # Expandable section for more samples
    with st.expander("🔍 Show more sample questions", expanded=st.session_state.show_more_samples):
        st.markdown("**Additional Examples:**")
        
        for i in range(0, len(secondary_questions), 2):
            col1, col2 = st.columns(2, gap="medium")
            
            with col1:
                if i < len(secondary_questions):
                    if st.button(
                        secondary_questions[i], 
                        key=f"sample_secondary_{i+1}", 
                        help="Click to use this sample question",
                        use_container_width=True
                    ):
                        populate_question_safe(secondary_questions[i])
            
            with col2:
                if i+1 < len(secondary_questions):
                    if st.button(
                        secondary_questions[i+1], 
                        key=f"sample_secondary_{i+2}", 
                        help="Click to use this sample question",
                        use_container_width=True
                    ):
                        populate_question_safe(secondary_questions[i+1])

def render_question_input_robust():
    """Render question input with robust state management"""
    
    st.markdown('<div style="margin: 24px 0;"></div>', unsafe_allow_html=True)
    
    st.markdown(f'''
        <section id="main-question-input" aria-labelledby="question-input-heading">
            <h3 id="question-input-heading" class="heading-tertiary">Your Question</h3>
        </section>
    ''', unsafe_allow_html=True)
    
    # Text area with stable key
    user_question_input = st.text_area(
        "Ask your question here:", 
        value=st.session_state.user_question, 
        height=120, 
        placeholder="e.g., 'Show me customers with declining sentiment trends who are at high risk of churn'", 
        key="user_question_input_main",
        help="Type your business question in natural language.",
        label_visibility="collapsed"
    )
    
    # Update session state synchronously
    if user_question_input != st.session_state.user_question:
        st.session_state.user_question = user_question_input
    
    # Action buttons
    st.markdown('<div style="margin: 16px 0;"></div>', unsafe_allow_html=True)
    
    button_col1, button_col2, spacer = st.columns([2, 1, 1], gap="medium")
    
    with button_col1:
        if st.button(
            "💬 Ask Cortex Analyst", 
            key="ask_button_main", 
            type="primary", 
            use_container_width=True,
            help="Submit your question to Cortex Analyst"
        ):
            question_to_ask = user_question_input if user_question_input else st.session_state.user_question
            if question_to_ask.strip():
                st.session_state.user_question = question_to_ask
                st.session_state.question_submitted = True
                # Use fragment-scoped rerun for immediate response
                st.rerun(scope="fragment")
            else:
                st.warning("⚠️ Please enter a question before submitting.")
    
    with button_col2:
        if st.button(
            "🧹 Clear Input", 
            key="clear_button_main", 
            use_container_width=True,
            help="Clear the input field"
        ):
            st.session_state.user_question = ""
            st.session_state.cortex_analyst_response = None
            # Use fragment-scoped rerun for immediate feedback
            st.rerun(scope="fragment")

def handle_api_calls_robust():
    """Handle API calls with robust error management"""
    
    if st.session_state.question_submitted and st.session_state.user_question:
        # Reset the flag immediately to prevent multiple calls
        st.session_state.question_submitted = False
        
        try:
            ask_cortex_analyst_api(st.session_state.user_question)
        except Exception as e:
            st.error(f"Error processing your question: {str(e)}")
            st.session_state.cortex_analyst_response = {
                "api_response_json": {"error": str(e)},
                "execution_time": 0,
                "request_id": None
            }
            # Use fragment-scoped rerun to update the display
            st.rerun(scope="fragment")

def render_response_robust():
    """Render response with stable container management"""
    
    st.markdown("---")
    st.markdown("""
        <section id="response-section" class="response-section" aria-labelledby="response-heading" role="region">
            <h2 id="response-heading" class="heading-secondary">🔍 Analyst Response</h2>
        </section>
    """, unsafe_allow_html=True)
    
    response_data = st.session_state.cortex_analyst_response
    display_cortex_response(
        response_data.get("api_response_json"), 
        response_data.get("execution_time"),
        response_data.get("request_id")
    )

def render_debug_information():
    """Render debug information for troubleshooting"""
    
    with st.expander("🔧 Fragment Debug Information", expanded=False):
        st.markdown("**Fragment State:**")
        col1, col2 = st.columns(2)
        with col1:
            st.write("Fragment Active:", st.session_state.get("fragment_active", False))
            st.write("Fragment Initialized:", st.session_state.get("fragment_initialized", False))
            st.write("Fragment Counter:", st.session_state.get("fragment_counter", 0))
        with col2:
            st.write("User Question Length:", len(st.session_state.get("user_question", "")))
            st.write("Has Response:", st.session_state.cortex_analyst_response is not None)
            st.write("Question Submitted:", st.session_state.get("question_submitted", False))
        
        st.markdown("**Session State Keys:**")
        st.write(list(st.session_state.keys()))
        
        st.markdown("**Performance Tips:**")
        st.info("✅ Using single fragment with stable keys")
        st.info("✅ Fragment lifecycle properly managed")
        st.info("✅ Graceful error handling for fragment conflicts")

def ask_cortex_analyst_api(question):
    """Query Cortex Analyst with robust error handling"""
    
    # Clear previous response
    st.session_state.cortex_analyst_response = None

    # Announce the start of processing to screen readers
    announce_to_screen_reader(f"Processing your question: {question}", "assertive")
    
    st.write(f"**Your Question:** {question}")
    start_time = time.time()
    
    # Show accessible loading announcement
    st.markdown(create_loading_announcement(
        "Analyzing your question with Cortex Analyst", 
        "30 seconds to 2 minutes"
    ), unsafe_allow_html=True)

    # Construct messages payload
    messages_payload = [{
        "role": "user",
        "content": [{"type": "text", "text": question}]
    }]

    payload = {
        "messages": messages_payload,
        "semantic_model_file": SEMANTIC_MODEL_PATH,
        "stream": False 
    }

    with st.spinner("Asking Cortex Analyst... This may take a moment."):
        try:
            if _IS_SNOWFLAKE_ENVIRONMENT:
                # Snowflake Environment
                if st.session_state.get("debug_mode"):
                    st.info("Running in Snowflake environment. Using _snowflake.send_snow_api_request.")

                timeout_ms = API_TIMEOUT_SECONDS * 1000
                
                snow_response = _snowflake.send_snow_api_request(
                    "POST",
                    SNOWFLAKE_CORTEX_ANALYST_API_PATH,
                    {},
                    {},
                    payload,
                    None,
                    timeout_ms
                )
                execution_time = time.time() - start_time
                
                response_status = snow_response.get("status")
                response_content_str = snow_response.get("content", "{}")
                api_response_json_data = json.loads(response_content_str)

                if response_status is not None and response_status < 400:
                    st.session_state.cortex_analyst_response = {
                        "api_response_json": api_response_json_data,
                        "execution_time": execution_time,
                        "request_id": api_response_json_data.get("request_id")
                    }
                    
                    announce_to_screen_reader(
                        f"Analysis complete. Response received in {execution_time:.1f} seconds.",
                        "polite"
                    )
                else:
                    error_message = f"Cortex Analyst API request failed (Snowflake Env): Status {response_status}"
                    if 'message' in api_response_json_data:
                        error_message += f" - {api_response_json_data.get('message', 'No details')}"
                    
                    st.error(error_message)
                    st.session_state.cortex_analyst_response = {
                        "api_response_json": {"error": error_message, "details": api_response_json_data},
                        "execution_time": execution_time,
                        "request_id": api_response_json_data.get("request_id")
                    }

            else:
                # Standalone Environment
                if st.session_state.get("debug_mode"):
                    st.info("Running in standalone environment. Using 'requests' library with PAT.")

                sf_user, pat_token, base_api_url, account_identifier = get_snowflake_credentials_and_url()
                if not (pat_token and base_api_url and account_identifier):
                    return 

                api_url = f"{base_api_url}{SNOWFLAKE_CORTEX_ANALYST_API_PATH}" 

                headers = {
                    "Authorization": f"Bearer {pat_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-Snowflake-Account": account_identifier,
                    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN"
                }
                
                response = requests.post(api_url, headers=headers, json=payload, timeout=API_TIMEOUT_SECONDS)
                execution_time = time.time() - start_time
                response.raise_for_status()
                
                api_response_json_data = response.json()

                st.session_state.cortex_analyst_response = {
                    "api_response_json": api_response_json_data,
                    "execution_time": execution_time,
                    "request_id": api_response_json_data.get("request_id")
                }
                
                announce_to_screen_reader(
                    f"Analysis complete. Response received in {execution_time:.1f} seconds.",
                    "polite"
                )

        except requests.exceptions.HTTPError as http_err:
            error_message = f"Cortex Analyst API request failed: {http_err.response.status_code}"
            error_details = {}
            try:
                error_details = http_err.response.json()
                error_message += f" - {error_details.get('message', 'No details')}"
            except json.JSONDecodeError:
                error_details = {"raw_text": str(http_err.response.text)}
                error_message += f" - Could not parse error JSON. Raw response: {http_err.response.text[:200]}..."
            
            st.error(error_message)
            st.session_state.cortex_analyst_response = {
                "api_response_json": {"error": error_message, "details": error_details},
                "execution_time": time.time() - start_time,
                "request_id": None
            }
            
        except requests.exceptions.RequestException as req_e:
            error_message = f"Error calling Cortex Analyst API: {req_e}"
            st.markdown(create_error_announcement(
                error_message, 
                "Please check your connection and try again."
            ), unsafe_allow_html=True)
            
            st.error(error_message)
            st.session_state.cortex_analyst_response = {
                "api_response_json": {"error": error_message, "details": str(req_e)},
                "execution_time": time.time() - start_time,
                "request_id": None
            }
            announce_to_screen_reader("An error occurred while processing your request", "assertive")
            
        except Exception as e:
            error_message = f"An unexpected error occurred: {e}"
            st.markdown(create_error_announcement(
                error_message, 
                "Please try again with a different question."
            ), unsafe_allow_html=True)
            
            st.error(error_message)
            st.session_state.cortex_analyst_response = {
                "api_response_json": {"error": error_message, "details": str(e)},
                "execution_time": time.time() - start_time,
                "request_id": None
            }
            announce_to_screen_reader("An unexpected error occurred", "assertive")
        
        finally:
            # Use fragment-scoped rerun to update display
            st.rerun(scope="fragment")

def get_snowflake_credentials_and_url():
    """Get Snowflake credentials for API access"""
    try: 
        account_identifier_env = None
        user_env = None
        pat_token_env = None
        source_log = []

        # Try Snowpark session first
        try:
            snowpark_conn = get_snowflake_connection()
            if hasattr(snowpark_conn, 'get_current_account') and hasattr(snowpark_conn, 'get_current_user'):
                current_account = snowpark_conn.get_current_account()
                current_user = snowpark_conn.get_current_user()
                if current_account and current_user:
                    account_identifier_env = current_account.strip('"').upper()
                    user_env = current_user.strip('"').upper()
                    source_log.append("Snowpark session for account/user")
        except Exception as e:
            if st.session_state.get("debug_mode"):
                st.info(f"Could not get account/user from Snowpark session: {e}")

        # Environment variables
        if not account_identifier_env:
            env_account = os.getenv("SNOWFLAKE_ACCOUNT")
            if env_account:
                account_identifier_env = env_account.upper()
                source_log.append("environment variables for account")
        
        if not user_env:
            env_user = os.getenv("SNOWFLAKE_USER")
            if env_user:
                user_env = env_user.upper()
                source_log.append("environment variables for user")
        
        env_pat_token = os.getenv("SNOWFLAKE_PAT_TOKEN")
        if env_pat_token:
            pat_token_env = env_pat_token
            source_log.append("environment variable for PAT")

        # Streamlit secrets
        if not pat_token_env or not account_identifier_env or not user_env:
            try:
                if hasattr(st, 'secrets'):
                    if "snowflake" in st.secrets:
                        snowflake_secrets = st.secrets.snowflake
                        if not account_identifier_env:
                            account_secret = snowflake_secrets.get("account")
                            if account_secret:
                                account_identifier_env = str(account_secret).upper()
                                source_log.append("Streamlit secrets for account")
                        if not user_env:
                            user_secret = snowflake_secrets.get("user")
                            if user_secret:
                                user_env = str(user_secret).upper()
                                source_log.append("Streamlit secrets for user")
                        if not pat_token_env:
                            pat_token_file = snowflake_secrets.get("pat_token_file")
                            if pat_token_file and os.path.exists(pat_token_file):
                                try:
                                    with open(pat_token_file, 'r') as f:
                                        pat_token_env = f.read().strip()
                                    source_log.append(f"PAT token file ({pat_token_file})")
                                except Exception as e:
                                    if st.session_state.get("debug_mode"):
                                        st.warning(f"Could not read PAT token from file {pat_token_file}: {e}")
                            else:
                                pat_secret = snowflake_secrets.get("pat_token")
                                if pat_secret:
                                    pat_token_env = str(pat_secret)
                                    source_log.append("Streamlit secrets for PAT")
            except Exception as e: 
                if st.session_state.get("debug_mode"): 
                    st.info(f"Error accessing Streamlit secrets: {e}")

        if source_log and st.session_state.get("debug_mode"):
            st.success(f"Sourced Snowflake connection details from: {', '.join(list(set(source_log)))}")
        
        if not account_identifier_env or not pat_token_env:
            error_parts = []
            if not account_identifier_env: error_parts.append("SNOWFLAKE_ACCOUNT")
            if not pat_token_env: error_parts.append("SNOWFLAKE_PAT_TOKEN")
            st.error(f"Missing credentials: {', '.join(error_parts)}")
            return None, None, None, None

        if not user_env and st.session_state.get("debug_mode"):
            st.info("SNOWFLAKE_USER not found, but not required for PAT authentication.")

        api_account_identifier = account_identifier_env.upper()
        api_user = user_env.upper() if user_env else None

        account_locator_for_url = api_account_identifier.split('.')[0].lower().replace("_", "-")
        base_api_url = f"https://{account_locator_for_url}.snowflakecomputing.com"
        
        if st.session_state.get("debug_mode"):
            st.success("Successfully sourced credentials for PAT-based API call.")
        
        return api_user, pat_token_env, base_api_url, api_account_identifier
        
    except Exception as e: 
        st.error(f"Critical error in get_snowflake_credentials_and_url: {e}")
        return None, None, None, None

def display_cortex_response(api_response_json, execution_time, request_id):
    """Display the structured response from Cortex Analyst API with stable rendering"""
    
    if not api_response_json:
        st.warning("No response data to display.")
        return

    if "error" in api_response_json:
        st.markdown("""
            <div class="card-outline" style="border-color: #ef4444; background-color: rgba(239, 68, 68, 0.1);">
                <h3 style="color: #ef4444; margin-bottom: 8px;">🚫 Error</h3>
            </div>
        """, unsafe_allow_html=True)
        
        error_msg_text = api_response_json.get('error', 'An unknown error occurred.')
        st.error(f"**{error_msg_text}**")
        
        if "details" in api_response_json and api_response_json['details']:
            with st.expander("🔍 Error Details", expanded=True):
                details_content = api_response_json['details']
                if isinstance(details_content, (dict, list)):
                    st.json(details_content)
                else:
                    st.code(str(details_content), language=None) 
        return

    analyst_message = api_response_json.get("message")
    if not analyst_message or "content" not in analyst_message:
        st.warning("Response format is not as expected.")
        st.json(api_response_json)
        return

    analyst_content = analyst_message["content"]
    
    generated_sql = None
    sql_confidence = None
    suggestions_list = []

    # Process response content
    for item_idx, item in enumerate(analyst_content):
        item_type = item.get("type")

        if item_type == "text":
            st.markdown("""
                <div class="card-outline" style="background-color: rgba(34, 197, 94, 0.05); border-color: #22c55e;">
                    <h3 class="heading-tertiary" style="color: #22c55e; margin-bottom: 16px;">💡 Answer</h3>
                </div>
            """, unsafe_allow_html=True)
            
            text_content = item.get("text", "No textual answer provided.")
            st.markdown(f"""
                <div style="
                    background-color: var(--bg-secondary);
                    padding: var(--space-md);
                    border-radius: 8px;
                    border-left: 4px solid #22c55e;
                    margin-bottom: var(--space-md);
                ">
                    <p style="margin: 0; font-size: var(--font-size-base); line-height: 1.6;">
                        {text_content}
                    </p>
                </div>
            """, unsafe_allow_html=True)
        
        elif item_type == "sql":
            generated_sql = item.get("statement")
            sql_confidence = item.get("confidence")
            if generated_sql:
                sql_expander_title = "📄 Generated SQL Query"
                confidence_indicator = ""
                
                if sql_confidence and sql_confidence.get("verified_query_used"):
                    confidence_indicator = " ✅ Verified"
                elif sql_confidence and sql_confidence.get("score") is not None:
                    confidence_score = sql_confidence.get("score")
                    if confidence_score > 0.7:
                        confidence_indicator = f" 🟢 High Confidence ({confidence_score:.2f})"
                    elif confidence_score > 0.4:
                        confidence_indicator = f" 🟡 Medium Confidence ({confidence_score:.2f})"
                    else:
                        confidence_indicator = f" 🔴 Low Confidence ({confidence_score:.2f})"

                with st.expander(f"{sql_expander_title}{confidence_indicator}", expanded=False):
                    st.code(generated_sql, language="sql")
                    
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        st.button("📋 Copy SQL", key=f"copy_sql_{request_id}_{item_idx}")

                    if sql_confidence:
                        if st.session_state.get("debug_mode") or not sql_confidence.get("verified_query_used"):
                            with st.expander("🔍 SQL Confidence Details", expanded=False):
                                st.json(sql_confidence)
                        elif sql_confidence.get("verified_query_used"):
                            st.success("✅ This query was answered using a pre-verified SQL pattern.")
        
        elif item_type == "suggestions":
            suggestions_list.extend(item.get("suggestions", []))

    # Execution time and request ID
    st.markdown(f"""
        <div class="text-secondary" style="text-align: center; margin: var(--space-md) 0;">
            ⏱️ Response retrieved in {execution_time:.2f} seconds | 
            🆔 Request ID: {request_id or 'N/A'}
        </div>
    """, unsafe_allow_html=True)

    # Query Results Section
    if generated_sql:
        st.markdown("""
            <div style="margin: var(--space-lg) 0;">
                <h3 class="heading-tertiary">📊 Query Results</h3>
            </div>
        """, unsafe_allow_html=True)
        
        try:
            df = run_query(generated_sql)
            
            if df.empty:
                st.info("ℹ️ Query returned no data, or an error occurred during execution.")
            else:
                # Results with stable tabs
                data_tab, chart_tab = st.tabs(["📄 Data", "📈 Visualization"])
                
                with data_tab:
                    st.markdown(f"""
                        <div class="card-outline" style="padding: var(--space-sm);">
                            <div class="text-secondary">
                                📝 Showing {len(df)} rows × {len(df.columns)} columns
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    st.dataframe(df, use_container_width=True)
                
                with chart_tab:
                    render_chart_visualization_stable(df, f"cortex_chart_{request_id or 'default'}")
                    
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
    
    # Follow-up Suggestions
    if suggestions_list:
        render_follow_up_suggestions_stable(suggestions_list, request_id)

def render_chart_visualization_stable(df: pd.DataFrame, chart_key_prefix: str):
    """Render chart visualization with stable keys"""
    
    if df.empty or len(df.columns) < 1:
        st.caption("🚫 Not enough data or columns to generate charts.")
        return
    
    try:
        # Initialize chart intelligence components
        analyzer = QueryResultAnalyzer()
        rule_engine = VisualizationRuleEngine()
        chart_selector = ChartSelector()
        
        # Analyze the dataset
        analysis = analyzer.analyze_result_set(df)
        
        # Get chart recommendations
        user_question = st.session_state.get('user_question', '')
        recommendations = rule_engine.recommend_charts(analysis, user_question)
        
        if not recommendations:
            st.warning("⚠️ No suitable chart recommendations found for this data.")
            return
        
        # Display data summary
        st.markdown(f"""
            <div style="background: rgba(34, 197, 94, 0.1); padding: 12px; border-radius: 8px; margin-bottom: 16px;">
                <h4 style="margin: 0 0 8px 0; color: #166534;">📊 Dataset Overview</h4>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; font-size: 14px;">
                    <div><strong>Rows:</strong> {analysis.row_count:,}</div>
                    <div><strong>Columns:</strong> {analysis.column_count}</div>
                    <div><strong>Numeric:</strong> {len(analysis.numeric_columns)}</div>
                    <div><strong>Categorical:</strong> {len(analysis.categorical_columns)}</div>
                    <div><strong>Temporal:</strong> {len(analysis.temporal_columns)}</div>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        # Render chart selector with stable key
        fig = chart_selector.render_chart_selector(
            recommendations=recommendations,
            data=df,
            analysis=analysis,
            key_prefix=f"{chart_key_prefix}_stable",
            query_context=user_question
        )
        
        # Display the chart
        if fig:
            st.plotly_chart(fig, use_container_width=True, key=f"{chart_key_prefix}_plotly_stable")
        
    except Exception as e:
        st.error(f"❌ Error in chart rendering: {str(e)}")

def render_follow_up_suggestions_stable(suggestions_list: list, request_id: str):
    """Render follow-up suggestions with stable keys"""
    
    st.markdown("""
        <div style="margin: var(--space-lg) 0;">
            <h3 class="heading-tertiary">🤔 You might also want to ask:</h3>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="card-outline">', unsafe_allow_html=True)
    
    # Display suggestions with stable keys
    for i, suggestion_text in enumerate(suggestions_list):
        if st.button(
            suggestion_text, 
            key=f"suggestion_{request_id}_{i}_stable",
            help="Click to ask this follow-up question",
            use_container_width=True
        ):
            # Update input and clear response
            st.session_state.user_question = suggestion_text
            st.session_state.cortex_analyst_response = None
            # Use fragment-scoped rerun
            st.rerun(scope="fragment")
    
    st.markdown('</div>', unsafe_allow_html=True)