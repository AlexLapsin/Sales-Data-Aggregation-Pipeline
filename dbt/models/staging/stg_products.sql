-- models/staging/stg_products.sql
-- Staging model for product master data

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ source('product_master', 'product_raw') }}
),

cleaned_data as (
    select
        -- Business key
        upper(trim(product_id)) as product_id,

        -- Product attributes
        trim(product_name) as product_name,
        upper(trim(category)) as category,
        upper(trim(sub_category)) as sub_category,
        trim(brand) as brand,

        -- Financial attributes
        unit_cost,
        unit_price,

        -- Calculate profit margin
        case
            when unit_price > 0
            then round((unit_price - coalesce(unit_cost, 0)) / unit_price, 4)
            else null
        end as profit_margin_pct,

        -- Lifecycle fields
        effective_date,
        coalesce(is_active, true) as is_active,
        ingestion_timestamp,

        -- Data quality checks
        case
            when product_id is null or trim(product_id) = '' then false
            when product_name is null or trim(product_name) = '' then false
            when unit_price is null or unit_price <= 0 then false
            when unit_cost is not null and unit_cost < 0 then false
            when unit_cost > unit_price then false  -- Cost shouldn't exceed price
            else true
        end as is_valid_record,

        -- Data quality score
        (
            case when product_id is not null and trim(product_id) != '' then 25 else 0 end +
            case when product_name is not null and trim(product_name) != '' then 20 else 0 end +
            case when category is not null then 15 else 0 end +
            case when sub_category is not null then 10 else 0 end +
            case when brand is not null then 10 else 0 end +
            case when unit_price > 0 then 15 else 0 end +
            case when unit_cost > 0 then 5 else 0 end
        ) as data_quality_score,

        -- Generate surrogate key
        {{ get_surrogate_key(['product_id', 'effective_date']) }} as product_raw_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from source_data
),

final as (
    select
        product_raw_key,
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        unit_cost,
        unit_price,
        profit_margin_pct,
        effective_date,
        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from cleaned_data

    -- Only include valid records with minimum quality
    where is_valid_record = true
      and data_quality_score >= 60
)

select * from final
