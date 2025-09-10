-- models/marts/fact_sales_daily.sql
-- Daily aggregated sales fact table for performance optimization

{{
  config(
    materialized='table',
    schema='marts',
    cluster_by=['date_key', 'store_key'],
    post_hook="create or replace view {{ target.schema }}.v_fact_sales_daily_current as select * from {{ this }} where date_key >= to_number(to_char(current_date() - interval '30 days', 'YYYYMMDD'))"
  )
}}

with fact_sales as (
    select * from {{ ref('fact_sales') }}
),

daily_aggregates as (
    select
        date_key,
        product_key,
        store_key,

        -- Aggregated measures
        count(*) as transaction_count,
        sum(quantity_sold) as total_quantity_sold,
        sum(gross_sales_amount) as total_gross_sales,
        sum(discount_amount) as total_discount_amount,
        sum(net_sales_amount) as total_net_sales,
        sum(cost_amount) as total_cost_amount,
        sum(profit_amount) as total_profit_amount,

        -- Calculated measures
        avg(net_sales_amount) as avg_transaction_size,
        avg(profit_margin_pct) as avg_profit_margin_pct,
        count(distinct customer_key) as unique_customers,

        -- Additional business metrics
        sum(case when discount_pct > 0 then 1 else 0 end) as discounted_transactions,
        sum(case when transaction_size_category = 'LARGE' or transaction_size_category = 'EXTRA_LARGE' then 1 else 0 end) as large_transactions,
        sum(case when quantity_category = 'BULK_PURCHASE' then 1 else 0 end) as bulk_purchases,

        -- Data quality metrics
        avg(data_quality_score) as avg_data_quality_score,
        count(case when data_source = 'STREAMING' then 1 end) as streaming_transactions,
        count(case when data_source = 'BATCH' then 1 end) as batch_transactions,

        -- Time-based metrics
        sum(case when time_of_day = 'MORNING' then 1 else 0 end) as morning_transactions,
        sum(case when time_of_day = 'AFTERNOON' then 1 else 0 end) as afternoon_transactions,
        sum(case when time_of_day = 'EVENING' then 1 else 0 end) as evening_transactions,
        sum(case when time_of_day = 'NIGHT' then 1 else 0 end) as night_transactions,

        -- Payment method distribution
        sum(case when payment_method = 'CREDIT_CARD' then 1 else 0 end) as credit_card_transactions,
        sum(case when payment_method = 'CASH' then 1 else 0 end) as cash_transactions,
        sum(case when payment_method = 'DIGITAL_WALLET' then 1 else 0 end) as digital_wallet_transactions,

        -- Audit fields
        min(created_at) as first_transaction_created,
        max(updated_at) as last_transaction_updated

    from fact_sales
    group by
        date_key,
        product_key,
        store_key
),

final as (
    select
        -- Generate daily sales key (matches existing Snowflake schema)
        row_number() over (order by date_key, product_key, store_key) as daily_sales_key,

        -- Dimension keys
        date_key,
        product_key,
        store_key,

        -- Aggregated measures
        transaction_count,
        total_quantity_sold,
        total_gross_sales,
        total_discount_amount,
        total_net_sales,
        total_cost_amount,
        total_profit_amount,

        -- Calculated measures
        avg_transaction_size,
        avg_profit_margin_pct,
        unique_customers,

        -- Additional business metrics
        discounted_transactions,
        round(discounted_transactions / transaction_count * 100, 2) as discount_penetration_pct,

        large_transactions,
        round(large_transactions / transaction_count * 100, 2) as large_transaction_pct,

        bulk_purchases,
        round(bulk_purchases / transaction_count * 100, 2) as bulk_purchase_pct,

        -- Data quality and source metrics
        avg_data_quality_score,
        streaming_transactions,
        batch_transactions,
        round(streaming_transactions / transaction_count * 100, 2) as streaming_mix_pct,

        -- Time distribution metrics
        morning_transactions,
        afternoon_transactions,
        evening_transactions,
        night_transactions,

        -- Peak time identification
        case
            when afternoon_transactions >= morning_transactions
                and afternoon_transactions >= evening_transactions
                and afternoon_transactions >= night_transactions
            then 'AFTERNOON'
            when evening_transactions >= morning_transactions
                and evening_transactions >= afternoon_transactions
                and evening_transactions >= night_transactions
            then 'EVENING'
            when morning_transactions >= afternoon_transactions
                and morning_transactions >= evening_transactions
                and morning_transactions >= night_transactions
            then 'MORNING'
            else 'NIGHT'
        end as peak_time_period,

        -- Payment method metrics
        credit_card_transactions,
        cash_transactions,
        digital_wallet_transactions,
        round(credit_card_transactions / transaction_count * 100, 2) as credit_card_pct,
        round(digital_wallet_transactions / transaction_count * 100, 2) as digital_wallet_pct,

        -- Performance indicators
        case
            when total_net_sales >= 10000 then 'HIGH_PERFORMANCE'
            when total_net_sales >= 5000 then 'GOOD_PERFORMANCE'
            when total_net_sales >= 1000 then 'AVERAGE_PERFORMANCE'
            else 'LOW_PERFORMANCE'
        end as daily_performance_tier,

        -- Audit columns
        first_transaction_created as created_at,
        last_transaction_updated as updated_at

    from daily_aggregates

    -- Only include days with reasonable transaction volume
    where transaction_count >= 1
      and avg_data_quality_score >= 70
)

select * from final
