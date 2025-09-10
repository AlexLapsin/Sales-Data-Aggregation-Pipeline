-- models/staging/stg_stores.sql
-- Staging model for store master data

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ source('store_master', 'store_raw') }}
),

cleaned_data as (
    select
        -- Business key
        upper(trim(store_id)) as store_id,

        -- Store attributes
        trim(store_name) as store_name,
        trim(store_location) as store_location,
        upper(trim(store_type)) as store_type,

        -- Geographic attributes
        trim(city) as city,
        upper(trim(state)) as state,
        upper(trim(country)) as country,
        upper(trim(region)) as region,

        -- Operational attributes
        opening_date,

        -- Lifecycle fields
        effective_date,
        coalesce(is_active, true) as is_active,
        ingestion_timestamp,

        -- Create standardized location string
        concat_ws(', ',
            nullif(trim(city), ''),
            nullif(trim(state), ''),
            nullif(trim(country), '')
        ) as full_location,

        -- Data quality validation
        case
            when store_id is null or trim(store_id) = '' then false
            when store_name is null or trim(store_name) = '' then false
            when opening_date > current_date() then false  -- Store can't open in future
            when effective_date is null then false
            else true
        end as is_valid_record,

        -- Data quality score
        (
            case when store_id is not null and trim(store_id) != '' then 25 else 0 end +
            case when store_name is not null and trim(store_name) != '' then 20 else 0 end +
            case when city is not null then 15 else 0 end +
            case when state is not null then 10 else 0 end +
            case when country is not null then 10 else 0 end +
            case when region is not null then 10 else 0 end +
            case when store_type is not null then 5 else 0 end +
            case when opening_date is not null then 5 else 0 end
        ) as data_quality_score,

        -- Business logic derivations
        case
            when opening_date <= current_date() - interval '5 years' then 'ESTABLISHED'
            when opening_date <= current_date() - interval '1 year' then 'MATURE'
            when opening_date <= current_date() - interval '3 months' then 'NEW'
            else 'OPENING_SOON'
        end as store_age_category,

        -- Generate surrogate key
        {{ get_surrogate_key(['store_id', 'effective_date']) }} as store_raw_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from source_data
),

final as (
    select
        store_raw_key,
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
        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from cleaned_data

    -- Filter for valid records with minimum quality
    where is_valid_record = true
      and data_quality_score >= 60
)

select * from final
