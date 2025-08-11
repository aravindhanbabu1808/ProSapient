select
    id as customer_id,
    first_name,
    last_name,
    email,
    gender,
    age,
    country,
    signup_date
from {{ source('raw', 'customers') }}
