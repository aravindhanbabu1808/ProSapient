{{ config(materialized='table') }}

select
    t.transaction_id,
    t.customer_id,
    t.product_id,
    t.transaction_date,
    t.quantity,
    p.price_gbp as unit_price_gbp,
    (t.quantity * p.price_gbp)::numeric as revenue_gbp
from {{ ref('stg_transactions') }} t
join {{ ref('stg_products') }} p
    on t.product_id = p.product_id
