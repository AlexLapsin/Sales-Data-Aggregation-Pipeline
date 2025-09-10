-- models/intermediate/int_product_scd2.sql
-- Slowly changing dimension type 2 logic for product data

{{
  config(
    materialized='table',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ ref('stg_products') }}
),

-- Get the latest record for each product to determine current state
latest_records as (
    select
        product_id,
        max(effective_date) as latest_effective_date
    from source_data
    group by product_id
),

-- Add row numbers to handle multiple records per day
ranked_data as (
    select
        s.*,
        l.latest_effective_date,
        row_number() over (
            partition by s.product_id, s.effective_date
            order by s.ingestion_timestamp desc
        ) as rn
    from source_data s
    inner join latest_records l on s.product_id = l.product_id
),

-- Remove duplicates (keep latest ingested record for each product/date)
deduped_data as (
    select *
    from ranked_data
    where rn = 1
),

-- Create SCD2 logic using window functions
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

        -- Calculate expiration date (next record's effective date - 1 day)
        coalesce(
            lead(effective_date) over (
                partition by product_id
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
            partition by product_id
            order by effective_date
        ) as version_number,

        -- Track what changed (for auditing)
        case
            when lag(product_name) over (partition by product_id order by effective_date) != product_name
                or lag(product_name) over (partition by product_id order by effective_date) is null
            then 'NAME_CHANGE'
            when lag(unit_price) over (partition by product_id order by effective_date) != unit_price
                or lag(unit_price) over (partition by product_id order by effective_date) is null
            then 'PRICE_CHANGE'
            when lag(unit_cost) over (partition by product_id order by effective_date) != unit_cost
                or lag(unit_cost) over (partition by product_id order by effective_date) is null
            then 'COST_CHANGE'
            when lag(category) over (partition by product_id order by effective_date) != category
                or lag(category) over (partition by product_id order by effective_date) is null
            then 'CATEGORY_CHANGE'
            else 'NEW_PRODUCT'
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
            when expiration_date = '2099-12-31' then null
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
