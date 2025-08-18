{{ config(materialized='view') }}

select
    id as product_id,
    name as product_name,
    category,
    price_gbp::numeric as price_gbp
from {{ source('raw', 'products') }}
