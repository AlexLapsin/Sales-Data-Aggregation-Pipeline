-- models/staging/stg_sales_batch.sql
-- Staging model for historical sales data from batch CSV processing

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select * from {{ source('raw_sales_data', 'sales_batch_raw') }}
),

cleaned_data as (
    select
        -- Business keys
        upper(trim(order_id)) as order_id,
        upper(trim(store_id)) as store_id,
        upper(trim(product_id)) as product_id,

        -- Product information
        trim(product_name) as product_name,
        upper(trim(category)) as category,

        -- Transaction details
        quantity,
        unit_price,
        total_price,
        sales as sales_amount,
        profit as profit_amount,

        -- Calculate derived fields to match streaming schema
        coalesce(
            case
                when sales_amount > 0 and quantity > 0
                then sales_amount / quantity
                else unit_price
            end,
            unit_price,
            0
        ) as calculated_unit_price,

        -- Calculate discount rate if not available
        case
            when sales_amount > 0 and total_price > 0
            then greatest(0, (total_price - sales_amount) / total_price)
            else 0.0
        end as discount_rate,

        -- Date handling
        order_date,
        ship_date,
        extract(hour from current_timestamp()) as order_hour,  -- Default to current hour since not available

        -- Geographic and customer info
        upper(trim(customer_segment)) as customer_segment,
        upper(trim(region)) as region,
        upper(trim(country)) as country,
        trim(state) as state,
        trim(city) as city,

        -- Data lineage
        source_file,
        batch_id,
        ingestion_timestamp,
        source_system,
        partition_date,

        -- Business rules validation
        case
            when quantity <= 0 then false
            when coalesce(unit_price, calculated_unit_price) <= 0 then false
            when coalesce(sales_amount, total_price) <= 0 then false
            when order_date > current_date() then false
            when ship_date < order_date then false
            else true
        end as is_valid_record,

        -- Data quality score
        (
            case when order_id is not null then 20 else 0 end +
            case when store_id is not null then 15 else 0 end +
            case when product_id is not null then 15 else 0 end +
            case when quantity > 0 then 15 else 0 end +
            case when sales_amount > 0 then 15 else 0 end +
            case when order_date is not null then 10 else 0 end +
            case when region is not null then 5 else 0 end +
            case when customer_segment is not null then 5 else 0 end
        ) as data_quality_score,

        -- Surrogate key
        {{ get_surrogate_key(['order_id', 'batch_id']) }} as sales_batch_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from source_data
),

standardized as (
    select
        sales_batch_key,
        order_id,
        store_id,
        product_id,
        null as customer_id,  -- Not available in batch data
        product_name,
        category,
        quantity,
        coalesce(unit_price, calculated_unit_price) as unit_price,
        coalesce(sales_amount, total_price) as total_price,
        discount_rate,

        -- Standardize amounts to match streaming schema
        quantity * coalesce(unit_price, calculated_unit_price) as gross_amount,
        quantity * coalesce(unit_price, calculated_unit_price) * discount_rate as discount_amount,
        coalesce(sales_amount, total_price) as net_amount,
        profit_amount,

        -- Convert date to timestamp for consistency with streaming
        order_date::timestamp_ntz as sale_timestamp,
        order_date as sale_date,
        order_hour as sale_hour,
        ship_date,

        -- Geographic info
        customer_segment,
        region,
        country,
        state,
        city,

        -- Use default payment method for batch data
        'UNKNOWN' as payment_method,
        concat_ws(', ', city, state, country) as store_location,

        -- Metadata
        source_file,
        batch_id,
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
),

final as (
    select
        sales_batch_key,
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
        profit_amount,
        sale_timestamp,
        sale_date,
        sale_hour,
        ship_date,
        payment_method,
        store_location,
        customer_segment,
        region,
        country,
        state,
        city,
        source_file,
        batch_id,
        ingestion_timestamp,
        source_system,
        partition_date,
        is_valid_record,
        data_quality_score,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from standardized

    -- Filter out invalid records
    where is_valid_record = true
      and data_quality_score >= 70
)

select * from final
