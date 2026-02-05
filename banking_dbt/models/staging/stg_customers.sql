select
    id as customer_id,
    first_name,
    last_name,
    email,
    created_at,
    current_timestamp as load_timestamp
from {{ source('raw', 'customers') }}