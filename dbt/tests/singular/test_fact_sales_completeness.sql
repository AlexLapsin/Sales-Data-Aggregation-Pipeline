-- tests/singular/test_fact_sales_completeness.sql
-- Ensure fact table has reasonable coverage compared to staging data

with staging_counts as (
    select
        sale_date,
        count(*) as staging_record_count,
        count(distinct order_id) as staging_order_count
    from {{ ref('int_sales_unified') }}
    where sale_date >= current_date() - interval '7 days'  -- Last 7 days
    group by sale_date
),

fact_counts as (
    select
        d.date_value as sale_date,
        count(*) as fact_record_count,
        count(distinct f.order_id) as fact_order_count
    from {{ ref('fact_sales') }} f
    inner join {{ ref('dim_date') }} d on f.date_key = d.date_key
    where d.date_value >= current_date() - interval '7 days'  -- Last 7 days
    group by d.date_value
),

completeness_check as (
    select
        s.sale_date,
        s.staging_record_count,
        s.staging_order_count,
        coalesce(f.fact_record_count, 0) as fact_record_count,
        coalesce(f.fact_order_count, 0) as fact_order_count,

        -- Calculate completeness ratios
        case
            when s.staging_record_count > 0
            then round((coalesce(f.fact_record_count, 0) / s.staging_record_count::float) * 100, 2)
            else 0
        end as record_completeness_pct,

        case
            when s.staging_order_count > 0
            then round((coalesce(f.fact_order_count, 0) / s.staging_order_count::float) * 100, 2)
            else 0
        end as order_completeness_pct

    from staging_counts s
    left join fact_counts f on s.sale_date = f.sale_date
)

-- This test fails if completeness is too low (< 85%)
select *
from completeness_check
where record_completeness_pct < 85
   or order_completeness_pct < 85
