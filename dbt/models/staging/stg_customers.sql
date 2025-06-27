-- Staging model for customer data
-- This model extracts and standardizes customer information from the raw JSON data
-- Fields are cast to appropriate data types and renamed for clarity

SELECT
    data:customer_id::VARCHAR AS customer_id,
    data:persona::VARCHAR AS persona,
    TRY_TO_DATE(data:sign_up_date::VARCHAR) AS sign_up_date,
    data:products_owned::NUMBER AS products_owned,
    data:lifetime_value::NUMBER AS lifetime_value
FROM {{ source('raw', 'customers') }} 