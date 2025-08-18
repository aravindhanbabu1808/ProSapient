{{ config(materialized='table') }}

select
    t.customer_id,
    sum(t.revenue_gbp) as total_revenue,
    count(distinct t.transaction_id) as order_count,
    count(distinct p.category) as category_count
from {{ ref('fct_transactions') }} t
join {{ ref('dim_products') }} p
    on t.product_id = p.product_id
group by t.customer_id
