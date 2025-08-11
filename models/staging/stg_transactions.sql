-- models/staging/stg_transactions.sql
select
  t.id as transaction_id,
  t.customer_id,
  t.product_id,
  p.name as product_name,
  p.category,
  t.quantity,
  case
    when t.transaction_date like '%/%'
      then to_date(t.transaction_date, 'DD/MM/YYYY')
    else
      to_date(t.transaction_date, 'YYYY-MM-DD')
  end::date as transaction_date
from {{ source('raw', 'transactions') }} as t
LEFT JOIN  {{ source('raw', 'products') }} as p
ON t.product_id=p.id