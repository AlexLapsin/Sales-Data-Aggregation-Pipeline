-- models/staging/stg_sales_raw.sql
-- Staging model for real-time sales data from Kafka

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ source('raw_sales_data', 'sales_raw') }}
),

cleaned_data as (
    select
        -- Business keys
        upper(trim(order_id)) as order_id,
        upper(trim(store_id)) as store_id,
        upper(trim(product_id)) as product_id,
        upper(trim(customer_id)) as customer_id,

        -- Product information
        trim(product_name) as product_name,
        upper(trim(category)) as category,

        -- Transaction details
        quantity,
        unit_price,
        total_price,
        coalesce(discount, 0.0) as discount_rate,

        -- Calculate derived fields
        quantity * unit_price as gross_amount,
        quantity * unit_price * discount as discount_amount,
        quantity * unit_price * (1 - coalesce(discount, 0.0)) as net_amount,

        -- Timestamps - ensure proper timezone handling
        sale_timestamp,
        date(sale_timestamp) as sale_date,
        extract(hour from sale_timestamp) as sale_hour,

        -- Customer and payment info
        upper(trim(payment_method)) as payment_method,
        trim(store_location) as store_location,

        -- Data lineage and quality
        ingestion_timestamp,
        source_system,
        partition_date,

        -- Add business rules validation flags
        case
            when quantity <= 0 then false
            when unit_price <= 0 then false
            when total_price <= 0 then false
            when sale_timestamp > current_timestamp() then false
            when discount < 0 or discount > 1 then false
            else true
        end as is_valid_record,

        -- Data quality score (0-100)
        (
            case when order_id is not null then 20 else 0 end +
            case when store_id is not null then 15 else 0 end +
            case when product_id is not null then 15 else 0 end +
            case when quantity > 0 then 15 else 0 end +
            case when unit_price > 0 then 15 else 0 end +
            case when total_price > 0 then 10 else 0 end +
            case when sale_timestamp is not null then 10 else 0 end
        ) as data_quality_score,

        -- Surrogate key for this record
        {{ get_surrogate_key(['order_id', 'sale_timestamp']) }} as sales_raw_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from source_data
),

final as (
    select
        sales_raw_key,
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
        sale_timestamp,
        sale_date,
        sale_hour,
        payment_method,
        store_location,
        ingestion_timestamp,
        source_system,
        partition_date,
        is_valid_record,
        data_quality_score,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from cleaned_data

    -- Filter out clearly invalid records
    where is_valid_record = true
      and data_quality_score >= 70  -- Require minimum data quality
)

select * from final
