-- models/staging/stg_customers_from_sales.sql
-- Derive unique customers with their locations from sales data (Silver layer)
-- Replaces stg_stores which referenced non-existent STORE_RAW table

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with sales_data as (
    select * from {{ ref('stg_sales_silver') }}
),

-- Get the most recent name/segment/location per customer_id
-- This handles data quality issues where same customer_id has multiple names
-- Prioritizes non-NULL names over recency to avoid NULL dimensions
customer_most_recent as (
    select
        customer_id,
        customer_name,
        customer_segment,
        city,
        state,
        country,
        postal_code,
        market,
        region,
        order_date,
        row_number() over (
            partition by customer_id
            order by
                CASE WHEN customer_name IS NOT NULL AND TRIM(customer_name) != '' THEN 0 ELSE 1 END,
                order_date desc,
                processing_timestamp desc
        ) as recency_rank
    from sales_data
    qualify recency_rank = 1
),

-- For backwards compatibility, rename to customer_location_mode
customer_location_mode as (
    select
        customer_id,
        customer_name,
        customer_segment,
        city,
        state,
        country,
        postal_code,
        market,
        region
    from customer_most_recent
),

customer_aggregates as (
    select
        s.customer_id,
        l.customer_name,  -- Use canonical name from most recent transaction
        l.customer_segment,  -- Use canonical segment from most recent transaction

        -- Most recent location from customer_location_mode CTE
        l.city as primary_city,
        l.state as primary_state,
        l.country as primary_country,
        l.postal_code as primary_postal_code,
        l.market as primary_market,
        l.region as primary_region,

        -- Purchase behavior
        count(distinct s.order_id) as total_orders,
        sum(s.quantity) as total_items_purchased,
        sum(s.sales_amount) as lifetime_value,
        avg(s.sales_amount) as avg_order_value,
        sum(s.profit_amount) as total_profit_generated,

        -- Recency metrics
        max(s.order_date) as last_order_date,
        min(s.order_date) as first_order_date,
        datediff(day, min(s.order_date), max(s.order_date)) as customer_tenure_days,

        -- Most recent processing
        max(s.processing_timestamp) as last_seen_timestamp

    from sales_data s
    inner join customer_location_mode l
        on s.customer_id = l.customer_id
    group by s.customer_id, l.customer_name, l.customer_segment,
             l.city, l.state, l.country, l.postal_code, l.market, l.region
),

cleaned_data as (
    select
        -- Business key
        upper(trim(customer_id)) as customer_id,

        -- Customer attributes
        trim(customer_name) as customer_name,
        upper(trim(customer_segment)) as customer_segment,

        -- Primary location
        trim(primary_city) as city,
        upper(trim(primary_state)) as state,
        upper(trim(primary_country)) as country,
        trim(primary_postal_code) as postal_code,
        upper(trim(primary_market)) as market,
        upper(trim(primary_region)) as region,

        -- Create standardized location string
        concat_ws(', ',
            nullif(trim(primary_city), ''),
            nullif(trim(primary_state), ''),
            nullif(trim(primary_country), '')
        ) as full_location,

        -- Purchase behavior metrics
        total_orders,
        total_items_purchased,
        lifetime_value,
        avg_order_value,
        total_profit_generated,

        -- Customer lifecycle
        first_order_date as effective_date,
        last_order_date,
        customer_tenure_days,

        case
            when last_order_date >= current_date() - interval '90 days' then true
            else false
        end as is_active,

        -- Customer value segmentation
        case
            when lifetime_value >= 10000 then 'PLATINUM'
            when lifetime_value >= 5000 then 'GOLD'
            when lifetime_value >= 1000 then 'SILVER'
            else 'BRONZE'
        end as value_tier,

        -- Customer age category
        case
            when customer_tenure_days >= 365 * 3 then 'LOYAL'
            when customer_tenure_days >= 365 then 'ESTABLISHED'
            when customer_tenure_days >= 90 then 'REGULAR'
            else 'NEW'
        end as tenure_category,

        last_seen_timestamp as ingestion_timestamp,

        -- Data quality validation
        case
            when customer_id is null or trim(customer_id) = '' then false
            when total_orders <= 0 then false
            when lifetime_value <= 0 then false
            when first_order_date is null then false
            else true
        end as is_valid_record,

        -- Data quality score
        (
            case when customer_id is not null and trim(customer_id) != '' then 20 else 0 end +
            case when customer_name is not null and trim(customer_name) != '' then 15 else 0 end +
            case when customer_segment is not null then 15 else 0 end +
            case when primary_city is not null then 10 else 0 end +
            case when primary_country is not null then 10 else 0 end +
            case when primary_region is not null then 10 else 0 end +
            case when total_orders > 0 then 10 else 0 end +
            case when lifetime_value > 0 then 10 else 0 end
        ) as data_quality_score,

        -- Generate surrogate key
        {{ get_surrogate_key(['customer_id']) }} as customer_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from customer_aggregates
),

final as (
    select
        customer_key,
        customer_id,
        customer_name,
        customer_segment,
        city,
        state,
        country,
        postal_code,
        market,
        region,
        full_location,
        total_orders,
        total_items_purchased,
        lifetime_value,
        avg_order_value,
        total_profit_generated,
        value_tier,
        tenure_category,
        effective_date,
        last_order_date,
        customer_tenure_days,
        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from cleaned_data

    -- Filter for valid customers with minimum quality
    where is_valid_record = true
      and data_quality_score >= 70
)

select * from final
