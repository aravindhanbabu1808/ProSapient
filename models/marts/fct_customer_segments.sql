-- models/marts/fct_customer_segments.sql

with cm as (  -- existing customer metrics
    select
        customer_id,
        total_revenue,
        order_count,
        category_count
    from {{ ref('customer_metrics') }}
),

-- NEW: gaps between purchases per customer
tx as (
    select
        t.customer_id,
        t.transaction_date::date as transaction_date
    from {{ ref('stg_transactions') }} t
),

gaps as (
    select
        customer_id,
        transaction_date,
        lag(transaction_date) over (
            partition by customer_id
            order by transaction_date
        ) as prev_transaction_date
    from tx
),

diffs as (
    select
        customer_id,
        (transaction_date - prev_transaction_date)::int as gap_days
    from gaps
    where prev_transaction_date is not null
),

avg_days as (  -- average gap per customer
    select
        customer_id,
        avg(gap_days)::numeric as avg_days_between_purchases
    from diffs
    group by customer_id
),

-- join avg_days into the customer metrics set
cm_enriched as (
    select
        c.*,
        a.avg_days_between_purchases
    from cm c
    left join avg_days a using (customer_id)
),

-- Revenue cutoffs
revenue_cutoffs as (
    select
        percentile_cont(0.333) within group (order by total_revenue) as p33,
        percentile_cont(0.667) within group (order by total_revenue) as p67
    from cm_enriched
),

-- Frequency cutoffs
freq_cutoffs as (
    select
        percentile_cont(0.333) within group (order by order_count) as p33,
        percentile_cont(0.667) within group (order by order_count) as p67
    from cm_enriched
),

-- Segmentation with scores (unchanged, now also carries avg_days_between_purchases)
scored as (
    select
        c.customer_id,
        c.total_revenue,
        c.order_count,
        c.category_count,
        c.avg_days_between_purchases,

        -- Spend segmentation
        case
            when c.total_revenue < rc.p33 then 'Low'
            when c.total_revenue < rc.p67 then 'Medium'
            else 'High'
        end as spend_segment,

        -- Frequency segmentation
        case
            when c.order_count < fc.p33 then 'Rare'
            when c.order_count < fc.p67 then 'Occasional'
            else 'Frequent'
        end as frequency_segment,

        -- Diversity segmentation
        case
            when c.category_count = 1 then 'Focused'
            else 'Diverse'
        end as diversity_segment,

        -- Numeric scores
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

        -- Combined score
        (case
            when c.total_revenue < rc.p33 then 1
            when c.total_revenue < rc.p67 then 2
            else 3
         end
         *
         case
            when c.order_count < fc.p33 then 1
            when c.order_count < fc.p67 then 2
            else 3
         end
        ) as combined_score

    from cm_enriched c
    cross join revenue_cutoffs rc
    cross join freq_cutoffs fc
),

-- Percentile cutoffs for combined score → final value segment
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
