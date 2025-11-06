-- models/intermediate/int_store_scd2.sql
-- Slowly changing dimension type 2 logic for store data

{{
  config(
    materialized='table',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ ref('stg_customers_from_sales') }}
),

-- Transform customer location data to store structure
transformed_data as (
    select
        -- Map customer to store (location-based entity)
        customer_id as store_id,
        customer_name as store_name,
        city as store_location,
        'RETAIL' as store_type,

        -- Location attributes
        city,
        state,
        country,
        region,
        full_location,

        -- Lifecycle dates
        effective_date as opening_date,

        -- Store age derived from customer tenure
        case
            when customer_tenure_days >= 365 * 3 then 'LEGACY'
            when customer_tenure_days >= 365 then 'ESTABLISHED'
            when customer_tenure_days >= 90 then 'MATURE'
            else 'NEW'
        end as store_age_category,

        -- SCD2 temporal fields (Type 1 semantics since Silver has no history)
        effective_date,  -- Use first_order_date from source (already mapped)
        '{{ var('scd2_end_date') }}'::date as expiration_date,
        true as is_current,
        1 as version_number,

        -- Change tracking (all records are new since no history)
        'NEW_STORE' as change_type,

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

scd2_data as (
    select * from transformed_data
),

final as (
    select
        {{ get_surrogate_key(['store_id', 'effective_date', 'version_number']) }} as store_scd2_key,
        store_id,
        store_name,
        store_location,
        store_type,
        city,
        state,
        country,
        region,
        full_location,
        opening_date,
        store_age_category,
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
            when datediff('year', opening_date, current_date()) >= 10 then 'LEGACY'
            when datediff('year', opening_date, current_date()) >= 5 then 'ESTABLISHED'
            when datediff('year', opening_date, current_date()) >= 2 then 'MATURE'
            when datediff('year', opening_date, current_date()) >= 1 then 'DEVELOPING'
            else 'NEW'
        end as maturity_level,

        -- Geographic groupings
        case
            when region in ('NORTH', 'NORTHEAST', 'NORTHWEST') then 'NORTHERN_REGIONS'
            when region in ('SOUTH', 'SOUTHEAST', 'SOUTHWEST') then 'SOUTHERN_REGIONS'
            when region in ('EAST', 'WEST') then 'COASTAL_REGIONS'
            when region in ('CENTRAL', 'MIDWEST') then 'CENTRAL_REGIONS'
            else 'OTHER_REGIONS'
        end as geographic_cluster,

        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at

    from scd2_data

    where is_valid_record = true
)

select * from final
