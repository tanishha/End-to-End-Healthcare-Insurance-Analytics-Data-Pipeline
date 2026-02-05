select
    id as account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    current_timestamp as load_timestamp
from {{ source('raw', 'accounts') }}