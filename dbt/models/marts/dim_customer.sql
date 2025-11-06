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

-- Get most recent location for each customer (Kimball best practice: consistent grain)
customer_latest_location as (
    select
        customer_id,
        customer_segment,
        region,
        country,
        state,
        city,
        sale_date,
        ingestion_timestamp,
        row_number() over (
            partition by customer_id
            order by sale_date desc, ingestion_timestamp desc
        ) as location_rank
    from unified_sales
    where customer_id is not null
      and customer_id != ''
),

-- Aggregate metrics at customer grain (ONE row per customer_id)
customer_base as (
    select
        s.customer_id,
        l.customer_segment,
        l.region,
        l.country,
        l.state,
        l.city,

        -- Aggregated customer behavior across ALL orders
        min(s.sale_date) as first_purchase_date,
        max(s.sale_date) as last_purchase_date,
        count(distinct s.order_id) as total_orders,
        sum(s.net_amount) as total_spent,
        avg(s.net_amount) as avg_order_value,
        sum(s.quantity) as total_items_purchased,

        -- Recency, Frequency, Monetary calculation
        datediff('day', max(s.sale_date), current_date()) as days_since_last_purchase,
        count(distinct s.order_id) as purchase_frequency,
        sum(s.net_amount) as monetary_value,

        -- Data quality
        max(s.enhanced_quality_score) as max_data_quality_score,
        min(s.ingestion_timestamp) as first_seen,
        max(s.ingestion_timestamp) as last_seen

    from unified_sales s
    inner join customer_latest_location l
        on s.customer_id = l.customer_id
        and l.location_rank = 1
    where s.customer_id is not null
      and s.customer_id != ''
    group by
        s.customer_id,
        l.customer_segment,
        l.region,
        l.country,
        l.state,
        l.city
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

        -- Customer value tier (pure monetary segmentation)
        case
            when monetary_value >= 10000 then 'VIP'
            when monetary_value >= 5000 then 'HIGH_VALUE'
            when monetary_value >= 1000 then 'REGULAR'
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
