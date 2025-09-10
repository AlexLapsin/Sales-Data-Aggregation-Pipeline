-- models/marts/dim_customer.sql
-- Customer dimension table derived from sales data

{{
  config(
    materialized='table',
    schema='marts',
    cluster_by=['customer_segment', 'is_active']
  )
}}

with unified_sales as (
    select * from {{ ref('int_sales_unified') }}
),

-- Extract unique customers with their attributes
customer_base as (
    select
        customer_id,
        customer_segment,
        region,
        country,
        state,
        city,

        -- Aggregated customer behavior
        min(sale_date) as first_purchase_date,
        max(sale_date) as last_purchase_date,
        count(distinct order_id) as total_orders,
        sum(net_amount) as total_spent,
        avg(net_amount) as avg_order_value,
        sum(quantity) as total_items_purchased,

        -- Recency, Frequency, Monetary calculation
        datediff('day', max(sale_date), current_date()) as days_since_last_purchase,
        count(distinct order_id) as purchase_frequency,
        sum(net_amount) as monetary_value,

        -- Data quality
        max(enhanced_quality_score) as max_data_quality_score,
        min(ingestion_timestamp) as first_seen,
        max(ingestion_timestamp) as last_seen

    from unified_sales
    where customer_id is not null
      and customer_id != ''
    group by
        customer_id,
        customer_segment,
        region,
        country,
        state,
        city
),

-- Add customer segmentation and categorization
customer_enriched as (
    select
        customer_id,
        coalesce(customer_segment, 'UNKNOWN') as customer_segment,
        'INDIVIDUAL' as customer_type,  -- Default assumption

        -- Demographics (placeholders for future enhancement)
        null as age_range,
        null as gender,
        null as income_bracket,

        -- Geographic info
        city,
        state,
        country,
        region,

        -- Behavioral attributes
        first_purchase_date,
        last_purchase_date,
        total_orders,
        total_spent,
        avg_order_value,
        total_items_purchased,
        days_since_last_purchase,
        purchase_frequency,
        monetary_value,

        -- Customer lifecycle
        case
            when days_since_last_purchase <= 30 then true
            else false
        end as is_active,

        -- RFM Segmentation
        case
            when days_since_last_purchase <= 30 then 'RECENT'
            when days_since_last_purchase <= 90 then 'MODERATE'
            when days_since_last_purchase <= 365 then 'DORMANT'
            else 'LOST'
        end as recency_segment,

        case
            when purchase_frequency >= 10 then 'HIGH_FREQUENCY'
            when purchase_frequency >= 5 then 'MEDIUM_FREQUENCY'
            when purchase_frequency >= 2 then 'LOW_FREQUENCY'
            else 'ONE_TIME'
        end as frequency_segment,

        case
            when monetary_value >= 1000 then 'HIGH_VALUE'
            when monetary_value >= 500 then 'MEDIUM_VALUE'
            when monetary_value >= 100 then 'LOW_VALUE'
            else 'MINIMAL_VALUE'
        end as monetary_segment,

        -- Customer value tier
        case
            when monetary_value >= 1000 and purchase_frequency >= 5 and days_since_last_purchase <= 90 then 'VIP'
            when monetary_value >= 500 and purchase_frequency >= 3 and days_since_last_purchase <= 180 then 'HIGH_VALUE'
            when monetary_value >= 100 and purchase_frequency >= 2 and days_since_last_purchase <= 365 then 'REGULAR'
            else 'OCCASIONAL'
        end as customer_value_tier,

        max_data_quality_score,
        first_seen,
        last_seen

    from customer_base
),

final as (
    select
        -- Surrogate key (matches existing Snowflake schema)
        row_number() over (order by customer_id) as customer_key,

        customer_id,
        null as customer_name,  -- Not available in source data
        customer_segment,
        customer_type,

        -- Demographics
        age_range,
        gender,
        income_bracket,

        -- Geographic info
        city,
        state,
        country,
        region,

        -- Lifecycle fields
        first_purchase_date,
        last_purchase_date,
        is_active,

        -- Customer behavior metrics
        total_orders,
        total_spent,
        avg_order_value,
        total_items_purchased,
        days_since_last_purchase,
        purchase_frequency,
        monetary_value,

        -- Segmentation
        recency_segment,
        frequency_segment,
        monetary_segment,
        customer_value_tier,

        -- Data quality and audit
        max_data_quality_score as data_quality_score,
        first_seen as ingestion_timestamp,

        -- Audit columns (matching existing schema)
        {{ get_current_timestamp() }} as created_at,
        {{ get_current_timestamp() }} as updated_at

    from customer_enriched

    -- Only include customers with reasonable data quality
    where max_data_quality_score >= 75
)

select * from final
