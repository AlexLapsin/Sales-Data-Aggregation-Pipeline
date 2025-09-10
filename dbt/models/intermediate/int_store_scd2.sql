-- models/intermediate/int_store_scd2.sql
-- Slowly changing dimension type 2 logic for store data

{{
  config(
    materialized='table',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ ref('stg_stores') }}
),

-- Get the latest record for each store
latest_records as (
    select
        store_id,
        max(effective_date) as latest_effective_date
    from source_data
    group by store_id
),

-- Add row numbers to handle multiple records per day
ranked_data as (
    select
        s.*,
        l.latest_effective_date,
        row_number() over (
            partition by s.store_id, s.effective_date
            order by s.ingestion_timestamp desc
        ) as rn
    from source_data s
    inner join latest_records l on s.store_id = l.store_id
),

-- Remove duplicates
deduped_data as (
    select *
    from ranked_data
    where rn = 1
),

-- Create SCD2 logic
scd2_data as (
    select
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

        -- Calculate expiration date
        coalesce(
            lead(effective_date) over (
                partition by store_id
                order by effective_date
            ) - interval '1 day',
            '2099-12-31'::date
        ) as expiration_date,

        -- Determine if this is the current record
        case
            when effective_date = latest_effective_date then true
            else false
        end as is_current,

        -- Create version number
        row_number() over (
            partition by store_id
            order by effective_date
        ) as version_number,

        -- Track what changed
        case
            when lag(store_name) over (partition by store_id order by effective_date) != store_name
                or lag(store_name) over (partition by store_id order by effective_date) is null
            then 'NAME_CHANGE'
            when lag(store_location) over (partition by store_id order by effective_date) != store_location
                or lag(store_location) over (partition by store_id order by effective_date) is null
            then 'LOCATION_CHANGE'
            when lag(store_type) over (partition by store_id order by effective_date) != store_type
                or lag(store_type) over (partition by store_id order by effective_date) is null
            then 'TYPE_CHANGE'
            when lag(region) over (partition by store_id order by effective_date) != region
                or lag(region) over (partition by store_id order by effective_date) is null
            then 'REGION_CHANGE'
            else 'NEW_STORE'
        end as change_type,

        latest_effective_date,
        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at

    from deduped_data
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
            when expiration_date = '2099-12-31' then null
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
