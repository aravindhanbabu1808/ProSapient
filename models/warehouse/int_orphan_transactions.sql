with orphan_txns as (
    select
        t.transaction_id,
        t.customer_id as txn_customer_id,  -- alias to avoid duplication
        t.product_id,
        t.quantity,
        t.transaction_date,
        c.customer_id as customer_table_id, -- alias to avoid duplication
        c.first_name,
        c.last_name,
        c.email,
        c.gender,
        c.age,
        c.country,
        c.signup_date
    from {{ ref('stg_transactions') }} t
    left join {{ ref('stg_customers') }} c
      on t.customer_id = c.customer_id
    where c.customer_id is null
)
select *
from orphan_txns
