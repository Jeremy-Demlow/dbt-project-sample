-- Staging model for support tickets
-- This model extracts and standardizes support ticket information from the raw JSON data
-- Fields are cast to appropriate data types and renamed for clarity

SELECT
    data:ticket_id::VARCHAR AS ticket_id,
    data:customer_id::VARCHAR AS customer_id,
    TRY_TO_TIMESTAMP_NTZ(data:ticket_date::VARCHAR) AS ticket_date,
    data:ticket_status::VARCHAR AS ticket_status,
    data:ticket_category::VARCHAR AS ticket_category,
    data:ticket_description::VARCHAR AS ticket_description
FROM {{ source('raw', 'support_tickets') }} 