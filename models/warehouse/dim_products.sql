{{ config(materialized='table') }}

select
    product_id,
    product_name,
    category,
    price_gbp
from {{ ref('stg_products') }}
