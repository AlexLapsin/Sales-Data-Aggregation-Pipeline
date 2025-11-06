-- models/intermediate/int_product_scd2.sql
-- Slowly changing dimension type 2 logic for product data

{{
  config(
    materialized='table',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ ref('stg_products_from_sales') }}
),

-- Transform source to match expected SCD2 structure
transformed_data as (
    select
        product_id,
        product_name,
        category,
        sub_category,

        -- Map derived fields to expected structure
        'UNKNOWN' as brand,
        round(unit_price * 0.70, 2) as unit_cost,
        unit_price,
        0.30 as profit_margin_pct,

        -- SCD2 temporal fields
        effective_date,  -- Use first_sale_date from source (already mapped)

        -- Pass through from source
        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at,
        effective_date as latest_effective_date

    from source_data
),

-- Deduplicate and assign is_current flag (only ONE current per product_id)
deduplicated_scd2 as (
    select
        *,
        row_number() over (
            partition by product_id
            order by effective_date desc, ingestion_timestamp desc
        ) as recency_rank
    from transformed_data
),

scd2_data as (
    select
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        unit_cost,
        unit_price,
        profit_margin_pct,
        effective_date,

        -- SCD2 logic: expiration date is next record's effective_date - 1 day, or far future for current
        coalesce(
            lead(effective_date) over (
                partition by product_id
                order by effective_date
            ) - interval '1 day',
            '{{ var('scd2_end_date') }}'::date
        ) as expiration_date,
        case when recency_rank = 1 then true else false end as is_current,
        recency_rank as version_number,

        -- Change tracking
        case when recency_rank = 1 then 'CURRENT_VERSION' else 'HISTORICAL_VERSION' end as change_type,

        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at,
        latest_effective_date
    from deduplicated_scd2
),

final as (
    select
        {{ get_surrogate_key(['product_id', 'effective_date', 'version_number']) }} as product_scd2_key,
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        unit_cost,
        unit_price,
        profit_margin_pct,
        effective_date,
        expiration_date,
        is_current,
        is_active,
        version_number,
        change_type,

        -- Calculate days this version was active
        case
            when expiration_date = '{{ var('scd2_end_date') }}' then null
            else datediff('day', effective_date, expiration_date) + 1
        end as days_active,

        -- Business categorizations
        case
            when unit_price < 10 then 'LOW_PRICE'
            when unit_price < 50 then 'MEDIUM_PRICE'
            when unit_price < 200 then 'HIGH_PRICE'
            else 'PREMIUM_PRICE'
        end as price_tier,

        case
            when profit_margin_pct < 0.1 then 'LOW_MARGIN'
            when profit_margin_pct < 0.3 then 'MEDIUM_MARGIN'
            when profit_margin_pct < 0.5 then 'HIGH_MARGIN'
            else 'PREMIUM_MARGIN'
        end as margin_tier,

        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at

    from scd2_data

    where is_valid_record = true
)

select * from final
