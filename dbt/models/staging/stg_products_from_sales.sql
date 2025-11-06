-- models/staging/stg_products_from_sales.sql
-- Derive unique products from sales data (Silver layer)
-- Replaces stg_products which referenced non-existent PRODUCT_RAW table

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with sales_data as (
    select * from {{ ref('stg_sales_silver') }}
),

-- Get most recent attributes for each product (Kimball best practice: consistent grain)
product_latest_attributes as (
    select
        product_id,
        product_name,
        category,
        sub_category,
        unit_price,
        sale_date,
        processing_timestamp,
        row_number() over (
            partition by product_id
            order by sale_date desc, processing_timestamp desc
        ) as attribute_rank
    from sales_data
    where product_id is not null
      and product_id != ''
),

-- Aggregate metrics at product grain (ONE row per product_id)
product_aggregates as (
    select
        s.product_id,
        a.product_name,
        a.category,
        a.sub_category,

        -- Aggregate financial metrics across ALL sales
        avg(s.unit_price) as avg_unit_price,
        min(s.unit_price) as min_unit_price,
        max(s.unit_price) as max_unit_price,
        count(distinct s.order_id) as total_orders,
        sum(s.quantity) as total_quantity_sold,
        sum(s.sales_amount) as total_sales,
        avg(s.profit_amount) as avg_profit_per_order,

        -- Lifecycle dates
        max(s.sale_date) as last_sale_date,
        min(s.sale_date) as first_sale_date,

        -- Audit
        max(s.processing_timestamp) as last_seen_timestamp

    from sales_data s
    inner join product_latest_attributes a
        on s.product_id = a.product_id
        and a.attribute_rank = 1
    where s.product_id is not null
      and s.product_id != ''
    group by
        s.product_id,
        a.product_name,
        a.category,
        a.sub_category
),

cleaned_data as (
    select
        -- Business key
        upper(trim(product_id)) as product_id,

        -- Product attributes
        trim(product_name) as product_name,
        upper(trim(category)) as category,
        upper(trim(sub_category)) as sub_category,

        -- Financial attributes (derived from sales history)
        avg_unit_price as unit_price,
        min_unit_price,
        max_unit_price,

        -- Calculate profit margin estimate (without cost data)
        case
            when avg_unit_price > 0
            then round(avg_profit_per_order / avg_unit_price, 4)
            else null
        end as estimated_profit_margin_pct,

        -- Sales performance
        total_orders,
        total_quantity_sold,
        total_sales,

        -- Lifecycle fields
        first_sale_date as effective_date,
        last_sale_date,
        case
            when last_sale_date >= current_date() - interval '30 days' then true
            else false
        end as is_active,

        last_seen_timestamp as ingestion_timestamp,

        -- Data quality checks
        case
            when product_id is null or trim(product_id) = '' then false
            when product_name is null or trim(product_name) = '' then false
            when avg_unit_price is null or avg_unit_price <= 0 then false
            when category is null then false
            else true
        end as is_valid_record,

        -- Data quality score
        (
            case when product_id is not null and trim(product_id) != '' then 25 else 0 end +
            case when product_name is not null and trim(product_name) != '' then 20 else 0 end +
            case when category is not null then 15 else 0 end +
            case when sub_category is not null then 10 else 0 end +
            case when avg_unit_price > 0 then 15 else 0 end +
            case when total_orders > 0 then 10 else 0 end +
            case when total_sales > 0 then 5 else 0 end
        ) as data_quality_score,

        -- Generate surrogate key
        {{ get_surrogate_key(['product_id']) }} as product_key,

        -- Audit columns
        {{ add_audit_columns() }}

    from product_aggregates
),

final as (
    select
        product_key,
        product_id,
        product_name,
        category,
        sub_category,
        unit_price,
        min_unit_price,
        max_unit_price,
        estimated_profit_margin_pct,
        total_orders,
        total_quantity_sold,
        total_sales,
        effective_date,
        last_sale_date,
        is_active,
        is_valid_record,
        data_quality_score,
        ingestion_timestamp,
        dbt_created_at,
        dbt_updated_at,
        dbt_invocation_id,
        dbt_target_name

    from cleaned_data

    -- Only include valid products with minimum quality
    where is_valid_record = true
      and data_quality_score >= 70
)

select * from final
