with tx as (
  select
    t.transaction_id,
    t.customer_id,
    t.product_id,
    t.quantity,
    date_trunc('month', t.transaction_date)::date as month
  from {{ ref('stg_transactions') }} t
),
priced as (
  select
    tx.*,
    p.price_gbp,
    tx.quantity * p.price_gbp as revenue
  from tx
  join {{ ref('stg_products') }} p 
    on tx.product_id = p.product_id
),
seg as (
  select * 
  from {{ ref('fct_customer_segments') }}
)
select
  s.spend_segment,
  s.frequency_segment,
  s.diversity_segment,
  p.month,
  sum(p.revenue) as monthly_revenue,
  count(distinct p.transaction_id) as monthly_orders,
  count(distinct p.customer_id) as active_customers
from priced p
join seg s 
  on s.customer_id = p.customer_id
group by 1,2,3,4
order by month
