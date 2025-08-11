with joined as (
    select
        t.customer_id,
        sum(t.quantity * p.price_gbp) as total_spent,
        count(t.transaction_id) as total_transactions,
        count(distinct p.category) as product_diversity
    from {{ ref('stg_transactions') }} t
    join {{ ref('stg_products') }} p
        on t.product_id = p.product_id
    group by t.customer_id
)

select * from joined
