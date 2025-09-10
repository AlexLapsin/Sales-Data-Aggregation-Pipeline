-- models/intermediate/int_sales_unified.sql
-- Unified sales model combining streaming and batch data sources

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with streaming_sales as (
    select
        sales_raw_key as source_key,
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
        null as profit_amount,  -- Not available in streaming data
        sale_timestamp,
        sale_date,
        sale_hour,
        null as ship_date,      -- Not available in streaming data
        payment_method,
        store_location,
        null as customer_segment,  -- Not available in streaming data
        null as region,         -- Not available in streaming data
        null as country,        -- Not available in streaming data
        null as state,          -- Not available in streaming data
        null as city,           -- Not available in streaming data
        'STREAMING' as data_source,
        source_system,
        ingestion_timestamp,
        partition_date,
        is_valid_record,
        data_quality_score,
        dbt_created_at,
        dbt_updated_at
    from {{ ref('stg_sales_raw') }}
),

batch_sales as (
    select
        sales_batch_key as source_key,
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
        'BATCH' as data_source,
        source_system,
        ingestion_timestamp,
        partition_date,
        is_valid_record,
        data_quality_score,
        dbt_created_at,
        dbt_updated_at
    from {{ ref('stg_sales_batch') }}
),

unified_sales as (
    select * from streaming_sales
    union all
    select * from batch_sales
),

enriched_sales as (
    select
        -- Generate unified surrogate key
        {{ get_surrogate_key(['order_id', 'data_source', 'source_key']) }} as unified_sales_key,

        source_key,
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

        -- Calculate profit if not available (simple estimation)
        coalesce(
            profit_amount,
            net_amount * 0.15  -- Assume 15% profit margin if not available
        ) as estimated_profit,

        -- Calculate profit margin
        case
            when net_amount > 0
            then coalesce(profit_amount, net_amount * 0.15) / net_amount
            else null
        end as profit_margin_pct,

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
            when sale_hour between 6 and 11 then 'MORNING'
            when sale_hour between 12 and 17 then 'AFTERNOON'
            when sale_hour between 18 and 21 then 'EVENING'
            else 'NIGHT'
        end as time_of_day,

        case
            when extract(dow from sale_date) in (0, 6) then 'WEEKEND'
            else 'WEEKDAY'
        end as day_type,

        -- Data lineage
        data_source,
        source_system,
        ingestion_timestamp,
        partition_date,
        is_valid_record,
        data_quality_score,

        -- Enhanced data quality score considering completeness
        data_quality_score +
        case when customer_id is not null then 5 else 0 end +
        case when profit_amount is not null then 5 else 0 end +
        case when region is not null then 5 else 0 end +
        case when customer_segment is not null then 5 else 0 end as enhanced_quality_score,

        dbt_created_at,
        dbt_updated_at

    from unified_sales
),

final as (
    select
        unified_sales_key,
        source_key,
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
      and enhanced_quality_score >= 75  -- Higher threshold for unified data
)

select * from final
