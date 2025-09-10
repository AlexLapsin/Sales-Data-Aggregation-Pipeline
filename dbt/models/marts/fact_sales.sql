-- models/marts/fact_sales.sql
-- Main sales fact table with transaction-level details

{{
  config(
    materialized='table',
    schema='marts',
    cluster_by=['date_key', 'store_key'],
    post_hook="create or replace view {{ target.schema }}.v_fact_sales_current as select * from {{ this }} where sale_date >= current_date() - interval '30 days'"
  )
}}

with unified_sales as (
    select * from {{ ref('int_sales_unified') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
    where is_current = true  -- Only current product versions for fact table
),

dim_store as (
    select * from {{ ref('dim_store') }}
    where is_current = true  -- Only current store versions for fact table
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
),

-- Create time dimension on the fly (simplified version)
dim_time as (
    select distinct
        sale_hour,
        sale_hour * 10000 as time_key,  -- Simple time key (HHMMSS format with MM=00, SS=00)
        case
            when sale_hour between 6 and 11 then 'MORNING'
            when sale_hour between 12 and 17 then 'AFTERNOON'
            when sale_hour between 18 and 21 then 'EVENING'
            else 'NIGHT'
        end as time_period,
        case when sale_hour between 9 and 17 then true else false end as business_hour_flag
    from unified_sales
),

fact_sales as (
    select
        s.unified_sales_key,

        -- Foreign keys to dimensions
        d.date_key,
        t.time_key,
        p.product_key,
        st.store_key,
        c.customer_key,

        -- Degenerate dimensions (transaction-level details that don't warrant their own dimension)
        s.order_id,
        s.payment_method,

        -- Additive measures (can be summed across any dimension)
        s.quantity as quantity_sold,
        s.gross_amount as gross_sales_amount,
        s.discount_amount,
        s.net_amount as net_sales_amount,
        s.unit_price * s.quantity - s.estimated_profit as cost_amount,  -- Estimated cost
        s.estimated_profit as profit_amount,

        -- Semi-additive measures (meaningful at specific grain)
        s.unit_price,
        s.unit_price * s.quantity - s.estimated_profit as unit_cost,  -- Estimated unit cost

        -- Non-additive measures (ratios and percentages)
        s.profit_margin_pct,
        s.discount_rate as discount_pct,

        -- Transaction context
        s.sale_timestamp,
        s.data_source as source_system,

        -- Business categorizations
        s.transaction_size_category,
        s.discount_category,
        s.quantity_category,
        s.time_of_day,
        s.day_type,

        -- Data quality indicators
        s.enhanced_quality_score as data_quality_score,
        s.data_source,

        -- Audit columns
        s.dbt_created_at as created_at,
        s.dbt_updated_at as updated_at

    from unified_sales s

    -- Join with dimension tables
    inner join dim_date d
        on s.sale_date = d.date_value

    left join dim_time t
        on s.sale_hour = t.sale_hour

    left join dim_product p
        on s.product_id = p.product_id
        and s.sale_date between p.effective_date and p.expiration_date

    left join dim_store st
        on s.store_id = st.store_id
        and s.sale_date between st.effective_date and st.expiration_date

    left join dim_customer c
        on s.customer_id = c.customer_id
),

final as (
    select
        -- Generate sales key (matches existing Snowflake schema)
        row_number() over (order by date_key, unified_sales_key) as sales_key,

        date_key,
        time_key,
        product_key,
        store_key,
        customer_key,

        -- Degenerate dimensions
        order_id,
        payment_method,

        -- Additive measures
        quantity_sold,
        gross_sales_amount,
        discount_amount,
        net_sales_amount,
        cost_amount,
        profit_amount,

        -- Semi-additive measures
        unit_price,
        coalesce(unit_cost, 0) as unit_cost,

        -- Non-additive measures
        profit_margin_pct,
        discount_pct,

        -- Transaction context
        sale_timestamp,
        source_system,

        -- Business categorizations (can be used for filtering and grouping)
        transaction_size_category,
        discount_category,
        quantity_category,
        time_of_day,
        day_type,
        data_source,
        data_quality_score,

        -- Audit columns
        created_at,
        updated_at

    from fact_sales

    -- Ensure we only include records with proper dimension lookups
    where product_key is not null
      and store_key is not null
      and date_key is not null
      and enhanced_quality_score >= 80  -- High quality threshold for fact table
)

select * from final
