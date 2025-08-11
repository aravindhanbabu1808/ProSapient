WITH customer_metrics AS (
    SELECT
        t.customer_id,
        SUM(t.quantity * p.price_gbp) AS total_revenue,
        COUNT(DISTINCT t.id) AS order_count,
        COUNT(DISTINCT p.category) AS category_count
    FROM transactions t
    JOIN customers c ON t.customer_id = c.id
    JOIN products p ON t.product_id = p.id
    GROUP BY t.customer_id
)
SELECT * FROM customer_metrics
