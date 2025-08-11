WITH customer_metrics AS (
    SELECT
        t.customer_id,
        SUM(t.quantity * p.price_gbp) AS total_revenue,
        COUNT(DISTINCT t.id) AS order_count,
        COUNT(DISTINCT p.category) AS category_count
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_products') }} p
        ON t.product_id = p.product_id
    GROUP BY t.customer_id
)

SELECT
    customer_id,
    total_revenue,
    order_count,
    category_count,
    CASE
        WHEN total_revenue >= high_revenue_cutoff THEN 'High'
        WHEN total_revenue >= medium_revenue_cutoff THEN 'Medium'
        ELSE 'Low'
    END AS revenue_segment,
    CASE
        WHEN order_count >= high_frequency_cutoff THEN 'Frequent'
        WHEN order_count >= medium_frequency_cutoff THEN 'Occasional'
        ELSE 'Rare'
    END AS frequency_segment,
    CASE
        WHEN category_count = 1 THEN 'Focused'
        ELSE 'Diverse'
    END AS diversity_segment
FROM customer_metrics
-- Cross join your cutoffs CTE if you’re still using dynamic bins for revenue/frequency
