{{ config(materialized='table') }}

with cm as (
    select * from {{ ref('int_customer_metrics') }}
),

tx as (
    select
        customer_id,
        transaction_date
    from {{ ref('fct_transactions') }}
),

gaps as (
    select
        customer_id,
        transaction_date,
        lag(transaction_date) over (
            partition by customer_id
            order by transaction_date
        ) as prev_date
    from tx
),

diffs as (
    select
        customer_id,
        (transaction_date - prev_date) as gap_days
    from gaps
    where prev_date is not null
),

avg_days as (
    select
        customer_id,
        avg(gap_days)::numeric as avg_days_between_purchases
    from diffs
    group by customer_id
),

cm_enriched as (
    select
        c.*,
        a.avg_days_between_purchases
    from cm c
    left join avg_days a using (customer_id)
),

revenue_cutoffs as (
    select
        percentile_cont(0.333) within group (order by total_revenue) as p33,
        percentile_cont(0.667) within group (order by total_revenue) as p67
    from cm_enriched
),

freq_cutoffs as (
    select
        percentile_cont(0.333) within group (order by order_count) as p33,
        percentile_cont(0.667) within group (order by order_count) as p67
    from cm_enriched
),

scored as (
    select
        c.customer_id,
        c.total_revenue,
        c.order_count,
        c.category_count,
        c.avg_days_between_purchases,

        case
            when c.total_revenue < rc.p33 then 'Low'
            when c.total_revenue < rc.p67 then 'Medium'
            else 'High'
        end as spend_segment,

        case
            when c.order_count < fc.p33 then 'Rare'
            when c.order_count < fc.p67 then 'Occasional'
            else 'Frequent'
        end as frequency_segment,

        case
            when c.category_count = 1 then 'Focused'
            else 'Diverse'
        end as diversity_segment,

        case
            when c.total_revenue < rc.p33 then 1
            when c.total_revenue < rc.p67 then 2
            else 3
        end as spend_score,

        case
            when c.order_count < fc.p33 then 1
            when c.order_count < fc.p67 then 2
            else 3
        end as frequency_score,

        (
            case when c.total_revenue < rc.p33 then 1
                 when c.total_revenue < rc.p67 then 2
                 else 3 end
            *
            case when c.order_count < fc.p33 then 1
                 when c.order_count < fc.p67 then 2
                 else 3 end
        ) as combined_score
    from cm_enriched c
    cross join revenue_cutoffs rc
    cross join freq_cutoffs fc
),

score_cutoffs as (
    select
        percentile_cont(0.333) within group (order by combined_score) as p33,
        percentile_cont(0.667) within group (order by combined_score) as p67
    from scored
)

select
    s.*,
    case
        when s.combined_score < sc.p33 then 'Low Value'
        when s.combined_score < sc.p67 then 'Medium Value'
        else 'High Value'
    end as final_value_segment
from scored s
cross join score_cutoffs sc
