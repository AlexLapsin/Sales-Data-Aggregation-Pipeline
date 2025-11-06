-- models/staging/stg_sales_silver.sql
-- Unified staging model for sales data from Delta Lake Silver layer via Snowflake Delta Direct
-- Replaces stg_sales_raw and stg_sales_batch with single source of truth

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ source('silver_delta_lake', 'sales_silver_external') }}
),

cleaned_data as (
    select
        -- Business keys (trust Silver layer standardization)
        order_id,
        customer_id,
        product_id,

        -- Order information
        order_date,
        ship_date,
        ship_mode,
        order_priority,

        -- Customer information
        customer_name,
        segment as customer_segment,
        city,
        state,
        country,
        postal_code,

        -- Geographic classification
        market,
        region,

        -- Product information
        product_name,
        category,
        sub_category,

        -- Transaction financial details
        quantity,
        sales as sales_amount,
        coalesce(discount, 0.0) as discount_rate,
        profit as profit_amount,
        shipping_cost,

        -- Calculate derived financial fields
        case
            when quantity > 0 then sales / quantity
            else 0
        end as unit_price,

        sales as gross_amount,
        sales * discount as discount_amount,
        sales * (1 - coalesce(discount, 0.0)) as net_amount,

        -- Timestamps and partitions
        order_date as sale_date,
        processing_timestamp,
        upper(trim(source_system)) as source_system,

        -- Data quality validations (already applied in Spark, validate again for safety)
        case
            when quantity <= 0 then false
            when sales <= 0 then false
            when order_date is null then false
            when ship_date is not null and ship_date < order_date then false
            when discount < 0 or discount > 1 then false
            else true
        end as is_valid_record,

        -- Data quality score (0-100)
        (
            case when order_id is not null then 15 else 0 end +
            case when customer_id is not null then 15 else 0 end +
            case when product_id is not null then 15 else 0 end +
            case when quantity > 0 then 10 else 0 end +
            case when sales > 0 then 10 else 0 end +
            case when order_date is not null then 10 else 0 end +
            case when customer_name is not null then 5 else 0 end +
            case when category is not null then 5 else 0 end +
            case when region is not null then 5 else 0 end +
            case when country is not null then 5 else 0 end +
            case when source_system in ('BATCH', 'KAFKA') then 5 else 0 end
        ) as data_quality_score,

        -- Generate surrogate key
        {{ get_surrogate_key(['order_id', 'product_id']) }} as sales_silver_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from source_data
),

final as (
    select
        sales_silver_key,
        order_id,
        customer_id,
        product_id,
        order_date,
        ship_date,
        ship_mode,
        order_priority,
        customer_name,
        customer_segment,
        city,
        state,
        country,
        postal_code,
        market,
        region,
        product_name,
        category,
        sub_category,
        quantity,
        unit_price,
        sales_amount,
        discount_rate,
        profit_amount,
        shipping_cost,
        gross_amount,
        discount_amount,
        net_amount,
        sale_date,
        processing_timestamp,
        source_system,
        is_valid_record,
        data_quality_score,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from cleaned_data

    -- Silver layer already validated, but double-check critical rules
    where is_valid_record = true
      and data_quality_score >= 70
)

select * from final
