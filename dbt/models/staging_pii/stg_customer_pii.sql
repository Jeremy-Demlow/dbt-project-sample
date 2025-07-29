{{
    config(
        materialized='table',
        schema='RAW_PII'
    )
}}

-- Sample customer PII data for testing classification
-- This creates synthetic PII data based on the existing customers table
WITH customer_json AS (
  SELECT parse_json(data) as customer_data
  FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_data:id::number as id,
    customer_data:first_name::string || ' ' || customer_data:last_name::string as name,
    customer_data:email::string as email,
    customer_data:phone_number::string as phone,
    customer_data:country::string || ', ' || customer_data:city::string || ', ' || customer_data:street_address::string as address,
    '4111-1111-1111-1111' as credit_card,
    '123-45-6789' as ssn
FROM customer_json 