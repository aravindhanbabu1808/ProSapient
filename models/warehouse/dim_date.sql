{{ config(materialized='table') }}

with calendar as (
    select
        d::date as date_day
    from generate_series(
        (select min(transaction_date) from {{ ref('stg_transactions') }}),
        (select max(transaction_date) from {{ ref('stg_transactions') }}),
        interval '1 day'
    ) d
)
select
    date_day as date,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    to_char(date_day, 'Month') as month_name,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    extract(quarter from date_day) as quarter
from calendar
