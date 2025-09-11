-- tests/singular/test_source_data_quality.sql
-- Comprehensive source data quality validation across all raw data sources

with streaming_quality as (
    select
        'STREAMING' as source_type,
        'INVALID_ORDER_IDS' as issue_type,
        count(*) as issue_count,
        'Order IDs with invalid format or length' as issue_description
    from {{ source('raw_sales_data', 'sales_raw') }}
    where order_id is null
       or length(trim(order_id)) < 5
       or order_id not regexp '[A-Z0-9]{2,}-[0-9]{4,}'

    union all

    select
        'STREAMING' as source_type,
        'FUTURE_TIMESTAMPS' as issue_type,
        count(*) as issue_count,
        'Sales timestamps in the future' as issue_description
    from {{ source('raw_sales_data', 'sales_raw') }}
    where sale_timestamp > current_timestamp()

    union all

    select
        'STREAMING' as source_type,
        'NEGATIVE_VALUES' as issue_type,
        count(*) as issue_count,
        'Negative quantities, prices, or totals' as issue_description
    from {{ source('raw_sales_data', 'sales_raw') }}
    where quantity <= 0 or unit_price <= 0 or total_price <= 0

    union all

    select
        'STREAMING' as source_type,
        'CALCULATION_MISMATCH' as issue_type,
        count(*) as issue_count,
        'Total price does not match quantity * unit_price * (1 - discount)' as issue_description
    from {{ source('raw_sales_data', 'sales_raw') }}
    where abs(total_price - (quantity * unit_price * (1 - coalesce(discount, 0)))) > 0.01

    union all

    select
        'STREAMING' as source_type,
        'MISSING_CRITICAL_FIELDS' as issue_type,
        count(*) as issue_count,
        'Records missing critical business keys' as issue_description
    from {{ source('raw_sales_data', 'sales_raw') }}
    where store_id is null or product_id is null
),

batch_quality as (
    select
        'BATCH' as source_type,
        'INVALID_DATE_ORDER' as issue_type,
        count(*) as issue_count,
        'Ship date before order date' as issue_description
    from {{ source('raw_sales_data', 'sales_batch_raw') }}
    where ship_date < order_date and ship_date is not null

    union all

    select
        'BATCH' as source_type,
        'OUTLIER_VALUES' as issue_type,
        count(*) as issue_count,
        'Extremely high unit prices or quantities' as issue_description
    from {{ source('raw_sales_data', 'sales_batch_raw') }}
    where unit_price > 50000 or quantity > 10000

    union all

    select
        'BATCH' as source_type,
        'INVALID_SEGMENTS' as issue_type,
        count(*) as issue_count,
        'Invalid customer segment values' as issue_description
    from {{ source('raw_sales_data', 'sales_batch_raw') }}
    where customer_segment not in ('CONSUMER', 'CORPORATE', 'HOME_OFFICE')
       and customer_segment is not null
),

product_quality as (
    select
        'PRODUCT_MASTER' as source_type,
        'PRICING_ANOMALIES' as issue_type,
        count(*) as issue_count,
        'Unit cost higher than unit price' as issue_description
    from {{ source('product_master', 'product_raw') }}
    where unit_cost > unit_price

    union all

    select
        'PRODUCT_MASTER' as source_type,
        'DUPLICATE_ACTIVE_PRODUCTS' as issue_type,
        count(*) - count(distinct product_id) as issue_count,
        'Multiple active records for same product' as issue_description
    from {{ source('product_master', 'product_raw') }}
    where is_active = true
    having count(*) > count(distinct product_id)
),

store_quality as (
    select
        'STORE_MASTER' as source_type,
        'FUTURE_OPENING_DATES' as issue_type,
        count(*) as issue_count,
        'Store opening dates in the future' as issue_description
    from {{ source('store_master', 'store_raw') }}
    where opening_date > current_date()

    union all

    select
        'STORE_MASTER' as source_type,
        'INVALID_STORE_TYPES' as issue_type,
        count(*) as issue_count,
        'Unrecognized store type values' as issue_description
    from {{ source('store_master', 'store_raw') }}
    where store_type not in ('FLAGSHIP', 'OUTLET', 'KIOSK', 'ONLINE', 'WAREHOUSE')
       and store_type is not null
),

all_quality_issues as (
    select * from streaming_quality
    union all
    select * from batch_quality
    union all
    select * from product_quality
    union all
    select * from store_quality
)

-- This test fails if any significant data quality issues are found
select *
from all_quality_issues
where issue_count > 0
