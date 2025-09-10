-- models/marts/dim_store.sql
-- Store dimension table with SCD2 implementation

{{
  config(
    materialized='table',
    schema='marts',
    cluster_by=['region', 'is_current']
  )
}}

with store_scd2 as (
    select * from {{ ref('int_store_scd2') }}
),

final as (
    select
        -- Surrogate key (matches existing Snowflake schema)
        row_number() over (order by store_id, effective_date) as store_key,

        store_id,
        store_name,
        store_location,
        city,
        state,
        country,
        region,
        store_type,
        opening_date,

        -- Geographic attributes (extended from existing schema)
        null as latitude,     -- Placeholder for future enhancement
        null as longitude,    -- Placeholder for future enhancement
        null as timezone,     -- Placeholder for future enhancement

        -- Store attributes (extended from existing schema)
        null as store_size_sqft,  -- Placeholder for future enhancement
        null as employee_count,   -- Placeholder for future enhancement

        -- SCD2 lifecycle fields
        effective_date,
        expiration_date,
        is_current,
        is_active,
        version_number,
        change_type,
        days_active,

        -- Business categorizations
        store_age_category,
        maturity_level,
        geographic_cluster,

        -- Additional business logic
        full_location,

        case
            when store_type in ('FLAGSHIP', 'WAREHOUSE') then 'LARGE_FORMAT'
            when store_type in ('OUTLET', 'KIOSK') then 'SMALL_FORMAT'
            else 'STANDARD_FORMAT'
        end as store_format,

        case
            when region in ('WEST', 'EAST') then 'COASTAL'
            when region in ('CENTRAL', 'MIDWEST') then 'INLAND'
            else 'OTHER'
        end as geographic_tier,

        -- Store performance indicators (to be enhanced with sales data)
        case
            when datediff('year', opening_date, current_date()) < 1 then 'RAMP_UP'
            when datediff('year', opening_date, current_date()) < 3 then 'GROWTH'
            when datediff('year', opening_date, current_date()) < 10 then 'MATURE'
            else 'LEGACY'
        end as lifecycle_stage,

        -- Data quality and audit
        data_quality_score,
        ingestion_timestamp,

        -- Audit columns (matching existing schema)
        dbt_created_at as created_at,
        dbt_updated_at as updated_at

    from store_scd2

    -- Only include valid, high-quality records
    where is_valid_record = true
      and data_quality_score >= 80
)

select * from final
