{{ config(materialized='view') }}

select
    id as customer_id,
    first_name,
    last_name,
    email,
    gender,
    age,
    country as country_code,
    signup_date::date as signup_date
from {{ source('raw', 'customers') }}
