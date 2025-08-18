{{ config(materialized='view') }}

select
    t.id as transaction_id,
    t.customer_id,
    t.product_id,
    t.quantity,
    case
        when t.transaction_date like '%/%'
            then to_date(t.transaction_date, 'DD/MM/YYYY')
        else
            to_date(t.transaction_date, 'YYYY-MM-DD')
    end as transaction_date
from {{ source('raw', 'transactions') }} t
