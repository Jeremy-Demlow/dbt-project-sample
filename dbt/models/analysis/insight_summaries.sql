-- Insight summaries model that generates AI summaries of customer interactions
-- This model uses Snowflake Cortex to generate concise summaries of customer behavior
-- Combines information from all interaction types into a single summary

SELECT
    customer_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        [
            {
                'role': 'user',
                'content': 'Summarize the following customer interactions in 100 words or less: ' || 
                ARRAY_TO_STRING(
                    ARRAY_AGG(
                        CASE
                            WHEN interaction_notes IS NOT NULL THEN interaction_notes
                            WHEN review_text IS NOT NULL THEN review_text
                            WHEN ticket_description IS NOT NULL THEN ticket_description
                        END
                    ),
                    ' | '
                )
            }
        ],
        {
            'max_tokens': 100
        }
    ) AS customer_summary
FROM (
    SELECT customer_id, interaction_notes, NULL as review_text, NULL as ticket_description
    FROM {{ ref('fact_customer_interactions') }}
    UNION ALL
    SELECT customer_id, NULL, review_text, NULL
    FROM {{ ref('fact_product_reviews') }}
    UNION ALL
    SELECT customer_id, NULL, NULL, ticket_description
    FROM {{ ref('fact_support_tickets') }}
)
GROUP BY customer_id 