-- tests/singular/test_dimensional_model_integrity.sql
-- Comprehensive dimensional model integrity testing for star schema compliance

with dimension_integrity as (
    -- Test 1: Ensure all dimension tables have proper surrogate keys
    select
        'DIM_DATE' as dimension_name,
        'MISSING_SURROGATE_KEYS' as issue_type,
        count(*) as issue_count
    from {{ ref('dim_date') }}
    where date_key is null

    union all

    select
        'DIM_PRODUCT' as dimension_name,
        'MISSING_SURROGATE_KEYS' as issue_type,
        count(*) as issue_count
    from {{ ref('dim_product') }}
    where product_key is null

    union all

    select
        'DIM_STORE' as dimension_name,
        'MISSING_SURROGATE_KEYS' as issue_type,
        count(*) as issue_count
    from {{ ref('dim_store') }}
    where store_key is null

    union all

    select
        'DIM_CUSTOMER' as dimension_name,
        'MISSING_SURROGATE_KEYS' as issue_type,
        count(*) as issue_count
    from {{ ref('dim_customer') }}
    where customer_key is null

    union all

    -- Test 2: Ensure SCD2 dimensions have only one current record per business key
    select
        'DIM_PRODUCT' as dimension_name,
        'MULTIPLE_CURRENT_RECORDS' as issue_type,
        count(*) as issue_count
    from (
        select product_id, count(*) as current_count
        from {{ ref('dim_product') }}
        where is_current = true
        group by product_id
        having count(*) > 1
    ) multiple_current

    union all

    select
        'DIM_STORE' as dimension_name,
        'MULTIPLE_CURRENT_RECORDS' as issue_type,
        count(*) as issue_count
    from (
        select store_id, count(*) as current_count
        from {{ ref('dim_store') }}
        where is_current = true
        group by store_id
        having count(*) > 1
    ) multiple_current

    /* Commented out: Orphaned dimensions are valid business scenarios
       (new products, stores not yet opened, seasonal items, etc.)

    union all

    -- Test 3: Check for orphaned dimension records (dimensions without facts)
    select
        'DIM_PRODUCT' as dimension_name,
        'ORPHANED_DIMENSION_RECORDS' as issue_type,
        count(*) as issue_count
    from {{ ref('dim_product') }}
    where is_current = true
      and product_key not in (
          select distinct product_key
          from {{ ref('fact_sales') }}
          where product_key is not null
      )

    union all

    select
        'DIM_STORE' as dimension_name,
        'ORPHANED_DIMENSION_RECORDS' as issue_type,
        count(*) as issue_count
    from {{ ref('dim_store') }}
    where is_current = true
      and store_key not in (
          select distinct store_key
          from {{ ref('fact_sales') }}
          where store_key is not null
      )
    */
),

fact_table_integrity as (
    -- Test 4: Check for orphaned fact records (facts without dimensions)
    select
        'FACT_SALES' as fact_name,
        'ORPHANED_FACT_RECORDS_PRODUCT' as issue_type,
        count(*) as issue_count
    from {{ ref('fact_sales') }}
    where product_key not in (
        select product_key
        from {{ ref('dim_product') }}
        where product_key is not null
    )

    union all

    select
        'FACT_SALES' as fact_name,
        'ORPHANED_FACT_RECORDS_STORE' as issue_type,
        count(*) as issue_count
    from {{ ref('fact_sales') }}
    where store_key not in (
        select store_key
        from {{ ref('dim_store') }}
        where store_key is not null
    )

    union all

    select
        'FACT_SALES' as fact_name,
        'ORPHANED_FACT_RECORDS_DATE' as issue_type,
        count(*) as issue_count
    from {{ ref('fact_sales') }}
    where date_key not in (
        select date_key
        from {{ ref('dim_date') }}
        where date_key is not null
    )

    union all

    -- Test 5: Check for facts with NULL measures (should not happen)
    select
        'FACT_SALES' as fact_name,
        'NULL_MEASURES' as issue_type,
        count(*) as issue_count
    from {{ ref('fact_sales') }}
    where quantity_sold is null
       or net_sales_amount is null
       or profit_amount is null

    union all

    -- Test 6: Check for negative quantities or zero sales amounts
    select
        'FACT_SALES' as fact_name,
        'INVALID_MEASURES' as issue_type,
        count(*) as issue_count
    from {{ ref('fact_sales') }}
    where quantity_sold <= 0 or net_sales_amount <= 0
),

aggregation_integrity as (
    -- Test 7: Ensure daily aggregate matches detail facts
    select
        'FACT_SALES_DAILY' as table_name,
        'AGGREGATION_MISMATCH' as issue_type,
        count(*) as issue_count
    from (
        -- Compare daily aggregates with sum of detail facts
        select
            d.date_key,
            d.product_key,
            d.store_key,
            d.total_net_sales,
            coalesce(f.detail_net_sales, 0) as detail_net_sales,
            abs(d.total_net_sales - coalesce(f.detail_net_sales, 0)) as variance
        from {{ ref('fact_sales_daily') }} d
        left join (
            select
                date_key,
                product_key,
                store_key,
                sum(net_sales_amount) as detail_net_sales
            from {{ ref('fact_sales') }}
            group by date_key, product_key, store_key
        ) f on d.date_key = f.date_key
            and d.product_key = f.product_key
            and d.store_key = f.store_key
        where abs(d.total_net_sales - coalesce(f.detail_net_sales, 0)) > 0.01
    ) mismatches

    union all

    -- Test 8: Check for impossible aggregations (more unique customers than transactions)
    select
        'FACT_SALES_DAILY' as table_name,
        'IMPOSSIBLE_AGGREGATIONS' as issue_type,
        count(*) as issue_count
    from {{ ref('fact_sales_daily') }}
    where unique_customers > transaction_count
),

all_integrity_issues as (
    select * from dimension_integrity
    union all
    select * from fact_table_integrity
    union all
    select * from aggregation_integrity
)

-- This test fails if any dimensional model integrity issues are found
select *
from all_integrity_issues
where issue_count > 0
