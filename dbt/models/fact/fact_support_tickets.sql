-- Fact model for support tickets with sentiment analysis and priority classification
-- This model enriches support tickets with AI-generated sentiment scores and priority levels
-- Uses Snowflake Cortex for sentiment analysis and text classification

SELECT
    t.ticket_id,
    t.customer_id,
    t.ticket_date,
    t.ticket_status,
    t.ticket_category,
    t.ticket_description,
    -- Add sentiment analysis
    SNOWFLAKE.CORTEX.SENTIMENT(t.ticket_description) AS sentiment_score,
    -- Add priority classification
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        t.ticket_description,
        [
            {
                'label': 'Critical',
                'description': 'Requires immediate attention and resolution',
                'examples': [
                    'System is down and customers cannot place orders',
                    'Security breach detected',
                    'Payment processing completely stopped',
                    'All users locked out of the system'
                ]
            },
            {
                'label': 'High',
                'description': 'Should be resolved within 24 hours',
                'examples': [
                    'Customer cannot complete checkout',
                    'Order status stuck in processing',
                    'Unable to access account',
                    'Payment failed for multiple customers'
                ]
            },
            {
                'label': 'Medium',
                'description': 'Should be resolved within 3 days',
                'examples': [
                    'Product image not displaying correctly',
                    'Slow response times in certain areas',
                    'Minor UI issues',
                    'Account settings not saving'
                ]
            },
            {
                'label': 'Low',
                'description': 'Can be handled in regular queue',
                'examples': [
                    'General product questions',
                    'Feature request',
                    'UI enhancement suggestion',
                    'Documentation clarification'
                ]
            }
        ],
        {
            'task_description': 'Classify the urgency level of this support ticket based on its description'
        }
    )['label'] AS priority_level,
    -- Add customer expectations
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        'What specific timeframe or deadline does the customer mention or expect for resolution? ' || t.ticket_description
    ) AS expected_resolution_timeframe,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        'What compensation, refund, or specific remedy is the customer seeking? ' || t.ticket_description
    ) AS requested_remedy
FROM {{ ref('stg_support_tickets') }} t
WHERE t.ticket_description IS NOT NULL 