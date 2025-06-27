-- Staging model for customer interactions
-- This model extracts and standardizes customer interaction information from the raw JSON data
-- Fields are cast to appropriate data types and renamed for clarity

SELECT
    data:interaction_id::VARCHAR AS interaction_id,
    data:customer_id::VARCHAR AS customer_id,
    TRY_TO_TIMESTAMP_NTZ(data:interaction_date::VARCHAR) AS interaction_date,
    data:agent_id::VARCHAR AS agent_id,
    data:interaction_type::VARCHAR AS interaction_type,
    data:interaction_notes::VARCHAR AS interaction_notes
FROM {{ source('raw', 'customer_interactions') }} 