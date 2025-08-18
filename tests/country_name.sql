-- tests/customer_country_valid.sql
with invalid as (
    select
        c.customer_id,
        c.country as country_code
    from {{ ref('stg_customers') }} c
    left join {{ ref('country_name') }} cn
        on c.country = cn.country_code
    where cn.country_code is null  -- no match in country seed
)

select * from invalid
