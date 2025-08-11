Customer Segmentation Project – dbt + PostgreSQL + Tableau
1. Purpose
This project processes customer purchase data to segment customers into meaningful groups, so the business can understand buying behaviour and target them better.
The results feed into a Tableau dashboard that shows segment distribution, trends, and insights.

2. How It Works
We use a dbt project with three layers:

Staging Layer – cleans raw data and makes it ready to use

stg_customers: cleans customer data

stg_products: cleans product data

stg_transactions: cleans transaction dates and links products/customers

Warehouse Layer – calculates base metrics

customer_metrics: total revenue, order count, and number of categories per customer

Mart Layer – applies business logic to create segments

fct_customer_segments: assigns Spend, Frequency, Diversity, and Final Value segments

Also calculates average days between purchases for each customer

3. Segmentation Rules
Spend Segment:

Low: bottom 33% of spend

Medium: middle 33%

High: top 33%

Frequency Segment:

Rare: bottom 33% of order counts

Occasional: middle 33%

Frequent: top 33%

Diversity Segment:

Focused: buys from only 1 category

Diverse: buys from more than 1 category

Final Value Segment:

Combines Spend and Frequency scores into Low / Medium / High Value groups

4. What You Can See in Tableau
Segment distribution (pie or bar charts)

Average purchase value per segment

Monthly revenue trends per segment

Top 3 product categories per segment

Customer movement between segments month-over-month

Average days between purchases per segment

How to Run the Project
bash
Copy
Edit
# Check connection
dbt debug

# Run everything
dbt build

# Just build staging models
dbt run --select staging

# View docs
dbt docs generate
dbt docs serve

Prices are in GBP

Customers or products missing from raw data are excluded from final segmentation

