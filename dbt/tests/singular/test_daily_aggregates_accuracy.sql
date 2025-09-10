-- tests/singular/test_daily_aggregates_accuracy.sql
-- Ensure daily fact table aggregations match detail fact table

with detail_aggregates as (
    select
        f.date_key,
        f.product_key,
        f.store_key,
        count(*) as expected_transaction_count,
        sum(f.quantity_sold) as expected_quantity_sold,
        sum(f.net_sales_amount) as expected_net_sales,
        sum(f.profit_amount) as expected_profit,
        count(distinct f.customer_key) as expected_unique_customers
    from {{ ref('fact_sales') }} f
    where f.date_key >= to_number(to_char(current_date() - interval '7 days', 'YYYYMMDD'))
    group by f.date_key, f.product_key, f.store_key
),

daily_aggregates as (
    select
        date_key,
        product_key,
        store_key,
        transaction_count,
        total_quantity_sold,
        total_net_sales,
        total_profit_amount,
        unique_customers
    from {{ ref('fact_sales_daily') }}
    where date_key >= to_number(to_char(current_date() - interval '7 days', 'YYYYMMDD'))
),

comparison as (
    select
        d.date_key,
        d.product_key,
        d.store_key,
        d.expected_transaction_count,
        coalesce(a.transaction_count, 0) as actual_transaction_count,
        d.expected_net_sales,
        coalesce(a.total_net_sales, 0) as actual_net_sales,
        d.expected_unique_customers,
        coalesce(a.unique_customers, 0) as actual_unique_customers,

        -- Calculate differences
        abs(d.expected_transaction_count - coalesce(a.transaction_count, 0)) as transaction_diff,
        abs(d.expected_net_sales - coalesce(a.total_net_sales, 0)) as sales_diff,
        abs(d.expected_unique_customers - coalesce(a.unique_customers, 0)) as customer_diff

    from detail_aggregates d
    left join daily_aggregates a
        on d.date_key = a.date_key
        and d.product_key = a.product_key
        and d.store_key = a.store_key
)

-- This test fails if aggregations don't match (allowing for small rounding differences)
select *
from comparison
where transaction_diff > 0
   or sales_diff > 0.01  -- Allow 1 cent rounding difference
   or customer_diff > 0
