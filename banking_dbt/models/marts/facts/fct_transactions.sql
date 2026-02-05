SELECT
    t.transaction_id,
    t.account_id,
    c.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_accounts') }} a
    ON t.account_id = a.account_id
    AND t.transaction_time >= a.effective_from
    AND (t.transaction_time < a.effective_to OR a.effective_to IS NULL)
LEFT JOIN {{ ref('dim_customers') }} c
    ON a.customer_id = c.customer_id
    AND t.transaction_time >= c.effective_from
    AND (t.transaction_time < c.effective_to OR c.effective_to IS NULL)