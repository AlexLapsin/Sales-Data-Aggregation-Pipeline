-- models/intermediate/int_sales_unified.sql
-- Unified sales model from Delta Lake Silver layer
-- Simplified: Now reads from single source of truth (stg_sales_silver) instead of combining streaming + batch

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with silver_sales as (
    select * from {{ ref('stg_sales_silver') }}
),

enriched_sales as (
    select
        -- Use existing sales_silver_key
        sales_silver_key as unified_sales_key,

        -- Business keys
        order_id,
        product_id,
        customer_id,

        -- Customer derived as store_id for compatibility with existing schema
        customer_id as store_id,

        -- Product information
        product_name,
        category,
        sub_category,

        -- Transaction financial details
        quantity,
        unit_price,
        sales_amount as total_price,
        discount_rate,
        gross_amount,
        discount_amount,
        net_amount,
        profit_amount,

        -- Estimated profit (use actual if available, otherwise estimate)
        coalesce(
            profit_amount,
            net_amount * 0.15  -- 15% profit margin estimate
        ) as estimated_profit,

        -- Profit margin calculation
        case
            when net_amount > 0
            then coalesce(profit_amount, net_amount * 0.15) / net_amount
            else null
        end as profit_margin_pct,

        -- Timestamps and dates
        sale_date,
        order_date,
        ship_date,
        processing_timestamp as sale_timestamp,
        extract(hour from processing_timestamp) as sale_hour,

        -- Customer and geographic information
        customer_name,
        customer_segment,
        city,
        state,
        country,
        postal_code,
        region,
        market,

        -- Order details
        ship_mode,
        order_priority,

        -- Derived fields for compatibility
        case when customer_name is not null
            then 'UNKNOWN'  -- Payment method not in Silver layer
            else 'UNKNOWN'
        end as payment_method,

        concat_ws(', ',
            nullif(trim(city), ''),
            nullif(trim(state), ''),
            nullif(trim(country), '')
        ) as store_location,

        -- Business categorizations
        case
            when net_amount < 50 then 'SMALL'
            when net_amount < 200 then 'MEDIUM'
            when net_amount < 500 then 'LARGE'
            else 'EXTRA_LARGE'
        end as transaction_size_category,

        case
            when discount_rate = 0 then 'NO_DISCOUNT'
            when discount_rate <= 0.1 then 'LOW_DISCOUNT'
            when discount_rate <= 0.25 then 'MEDIUM_DISCOUNT'
            else 'HIGH_DISCOUNT'
        end as discount_category,

        case
            when quantity = 1 then 'SINGLE_ITEM'
            when quantity <= 5 then 'FEW_ITEMS'
            when quantity <= 10 then 'MULTIPLE_ITEMS'
            else 'BULK_PURCHASE'
        end as quantity_category,

        -- Time-based categorizations
        case
            when extract(hour from processing_timestamp) between 6 and 11 then 'MORNING'
            when extract(hour from processing_timestamp) between 12 and 17 then 'AFTERNOON'
            when extract(hour from processing_timestamp) between 18 and 21 then 'EVENING'
            else 'NIGHT'
        end as time_of_day,

        case
            when extract(dow from order_date) in (0, 6) then 'WEEKEND'
            else 'WEEKDAY'
        end as day_type,

        -- Data lineage
        source_system as data_source,
        source_system,
        processing_timestamp as ingestion_timestamp,
        order_date as partition_date,  -- Use order_date for partitioning
        is_valid_record,
        data_quality_score,

        -- Enhanced data quality score considering completeness
        data_quality_score +
        case when customer_id is not null and customer_id != '' then 5 else 0 end +
        case when profit_amount is not null then 5 else 0 end +
        case when region is not null then 5 else 0 end +
        case when customer_segment is not null then 5 else 0 end as enhanced_quality_score,

        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from silver_sales
),

final as (
    select
        unified_sales_key,
        order_id,
        store_id,
        product_id,
        customer_id,
        product_name,
        category,
        quantity,
        unit_price,
        total_price,
        discount_rate,
        gross_amount,
        discount_amount,
        net_amount,
        profit_amount,
        estimated_profit,
        profit_margin_pct,
        sale_timestamp,
        sale_date,
        sale_hour,
        ship_date,
        payment_method,
        store_location,
        customer_segment,
        region,
        country,
        state,
        city,
        transaction_size_category,
        discount_category,
        quantity_category,
        time_of_day,
        day_type,
        data_source,
        source_system,
        ingestion_timestamp,
        partition_date,
        is_valid_record,
        data_quality_score,
        enhanced_quality_score,
        dbt_created_at,
        dbt_updated_at

    from enriched_sales

    -- Apply final quality filters
    where is_valid_record = true
      and enhanced_quality_score >= 75
)

select * from final
