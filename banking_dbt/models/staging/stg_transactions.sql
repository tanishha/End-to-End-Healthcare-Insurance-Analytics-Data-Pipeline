select
    id as transaction_id,
    account_id,
    amount,
    txn_type as transaction_type,
    related_account_id,
    status,
    created_at as transaction_time,
    current_timestamp as load_timestamp
from {{ source('raw', 'transactions') }}