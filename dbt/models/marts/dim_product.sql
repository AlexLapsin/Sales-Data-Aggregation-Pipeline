-- models/marts/dim_product.sql
-- Product dimension table with SCD2 implementation

{{
  config(
    materialized='table',
    schema='marts',
    cluster_by=['category', 'is_current']
  )
}}

with product_scd2 as (
    select * from {{ ref('int_product_scd2') }}
),

final as (
    select
        -- Surrogate key (matches existing Snowflake schema)
        row_number() over (order by product_id, effective_date) as product_key,

        product_id,
        product_name,
        category,
        sub_category,
        brand,
        unit_cost,
        unit_price,
        profit_margin_pct,

        -- Product attributes (extended from existing schema)
        null as product_size,    -- Placeholder for future enhancement
        null as product_color,   -- Placeholder for future enhancement
        null as product_weight,  -- Placeholder for future enhancement

        -- SCD2 lifecycle fields
        effective_date,
        expiration_date,
        is_current,
        is_active,
        version_number,
        change_type,
        days_active,

        -- Business categorizations
        price_tier,
        margin_tier,

        -- Additional business logic
        case
            when category in ('TECHNOLOGY', 'ELECTRONICS') then 'HIGH_TECH'
            when category in ('FURNITURE', 'OFFICE_SUPPLIES') then 'TRADITIONAL'
            else 'OTHER'
        end as product_family,

        case
            when unit_price / nullif(unit_cost, 0) >= 3 then 'HIGH_MARKUP'
            when unit_price / nullif(unit_cost, 0) >= 2 then 'MEDIUM_MARKUP'
            when unit_price / nullif(unit_cost, 0) >= 1.5 then 'LOW_MARKUP'
            else 'MINIMAL_MARKUP'
        end as markup_category,

        -- Data quality and audit
        data_quality_score,
        ingestion_timestamp,

        -- Audit columns (matching existing schema)
        dbt_created_at as created_at,
        dbt_updated_at as updated_at

    from product_scd2

    -- Only include valid, high-quality records
    where is_valid_record = true
      and data_quality_score >= 80
)

select * from final
