{% snapshot accounts_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='account_id',
      strategy='check',
      check_cols=['balance', 'account_type', 'currency']
    )
}}
SELECT 
    id as account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at
FROM {{ source('raw', 'accounts') }}
{% endsnapshot %}
