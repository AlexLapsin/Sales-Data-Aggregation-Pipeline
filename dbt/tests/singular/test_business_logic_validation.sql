-- tests/singular/test_business_logic_validation.sql
-- Comprehensive business logic validation across all transformation layers

with staging_business_rules as (
    -- Rule 1: Valid discount rates (0-100%)
    select
        'STAGING' as layer,
        'INVALID_DISCOUNT_RATES' as rule_type,
        'stg_sales_raw' as model_name,
        count(*) as violation_count
    from {{ ref('stg_sales_raw') }}
    where discount_rate < 0 or discount_rate > 1

    union all

    -- Rule 2: Sales amount calculations should be consistent
    select
        'STAGING' as layer,
        'AMOUNT_CALCULATION_ERROR' as rule_type,
        'stg_sales_raw' as model_name,
        count(*) as violation_count
    from {{ ref('stg_sales_raw') }}
    where abs(gross_amount - (quantity * unit_price)) > 0.01
       or abs(net_amount - (gross_amount - discount_amount)) > 0.01

    union all

    -- Rule 3: Data quality score should reflect actual data completeness
    select
        'STAGING' as layer,
        'INCORRECT_QUALITY_SCORE' as rule_type,
        'stg_sales_raw' as model_name,
        count(*) as violation_count
    from {{ ref('stg_sales_raw') }}
    where data_quality_score < 70  -- Minimum threshold not met
       or (data_quality_score = 100 and (
           order_id is null or store_id is null or product_id is null or
           quantity <= 0 or unit_price <= 0 or sale_date is null
       ))
),

intermediate_business_rules as (
    -- Rule 4: Unified sales should not have duplicate orders from same source
    select
        'INTERMEDIATE' as layer,
        'DUPLICATE_UNIFIED_RECORDS' as rule_type,
        'int_sales_unified' as model_name,
        count(*) - count(distinct order_id, data_source) as violation_count
    from {{ ref('int_sales_unified') }}
    having count(*) > count(distinct order_id, data_source)

    union all

    -- Rule 5: Transaction size categories should match actual amounts
    select
        'INTERMEDIATE' as layer,
        'INCORRECT_SIZE_CATEGORY' as rule_type,
        'int_sales_unified' as model_name,
        count(*) as violation_count
    from {{ ref('int_sales_unified') }}
    where (transaction_size_category = 'SMALL' and net_amount >= 100)
       or (transaction_size_category = 'MEDIUM' and (net_amount < 100 or net_amount >= 500))
       or (transaction_size_category = 'LARGE' and (net_amount < 500 or net_amount >= 1000))
       or (transaction_size_category = 'EXTRA_LARGE' and net_amount < 1000)

    union all

    -- Rule 6: SCD2 effective dates must be logical
    select
        'INTERMEDIATE' as layer,
        'INVALID_SCD2_DATES' as rule_type,
        'int_product_scd2' as model_name,
        count(*) as violation_count
    from {{ ref('int_product_scd2') }}
    where effective_date > expiration_date
       or (is_current = true and expiration_date != '2099-12-31')
       or (is_current = false and expiration_date = '2099-12-31')

    union all

    select
        'INTERMEDIATE' as layer,
        'INVALID_SCD2_DATES' as rule_type,
        'int_store_scd2' as model_name,
        count(*) as violation_count
    from {{ ref('int_store_scd2') }}
    where effective_date > expiration_date
       or (is_current = true and expiration_date != '2099-12-31')
       or (is_current = false and expiration_date = '2099-12-31')
),

marts_business_rules as (
    -- Rule 7: Fact table foreign keys must exist in dimensions
    select
        'MARTS' as layer,
        'BROKEN_REFERENTIAL_INTEGRITY' as rule_type,
        'fact_sales' as model_name,
        count(*) as violation_count
    from {{ ref('fact_sales') }} f
    where not exists (
        select 1 from {{ ref('dim_date') }} d
        where d.date_key = f.date_key
    )

    union all

    -- Rule 8: Profit calculations should be reasonable
    select
        'MARTS' as layer,
        'UNREALISTIC_PROFIT_MARGINS' as rule_type,
        'fact_sales' as model_name,
        count(*) as violation_count
    from {{ ref('fact_sales') }}
    where profit_amount / net_sales_amount > 0.95  -- >95% profit margin
       or profit_amount / net_sales_amount < -2.0  -- Loss >200% of sales

    union all

    -- Rule 9: Customer segmentation logic
    select
        'MARTS' as layer,
        'INCORRECT_CUSTOMER_TIERS' as rule_type,
        'dim_customer' as model_name,
        count(*) as violation_count
    from {{ ref('dim_customer') }}
    where (customer_value_tier = 'VIP' and total_spent < 10000)
       or (customer_value_tier = 'HIGH_VALUE' and (total_spent < 5000 or total_spent >= 10000))
       or (customer_value_tier = 'REGULAR' and (total_spent < 1000 or total_spent >= 5000))
       or (customer_value_tier = 'OCCASIONAL' and total_spent >= 1000)

    union all

    -- Rule 10: Date dimension completeness and accuracy
    select
        'MARTS' as layer,
        'DATE_DIMENSION_LOGIC_ERROR' as rule_type,
        'dim_date' as model_name,
        count(*) as violation_count
    from {{ ref('dim_date') }}
    where (is_weekend = true and day_of_week not in (0, 6))
       or (is_weekend = false and day_of_week in (0, 6))
       or quarter not between 1 and 4
       or month_number not between 1 and 12
       or day_of_week not between 0 and 6

    union all

    -- Rule 11: Daily aggregates should sum correctly
    select
        'MARTS' as layer,
        'DAILY_AGGREGATION_ERROR' as rule_type,
        'fact_sales_daily' as model_name,
        sum(case when variance > 0.01 then 1 else 0 end) as violation_count
    from (
        select
            date_key,
            product_key,
            store_key,
            abs(total_net_sales - coalesce(actual_sum, 0)) as variance
        from {{ ref('fact_sales_daily') }} daily
        left join (
            select
                date_key,
                product_key,
                store_key,
                sum(net_sales_amount) as actual_sum
            from {{ ref('fact_sales') }}
            group by date_key, product_key, store_key
        ) detail on daily.date_key = detail.date_key
                  and daily.product_key = detail.product_key
                  and daily.store_key = detail.store_key
    ) variance_check
),

cross_layer_validation as (
    -- Rule 12: Data should flow consistently through transformation layers
    select
        'CROSS_LAYER' as layer,
        'DATA_LOSS_IN_PIPELINE' as rule_type,
        'staging_to_facts' as model_name,
        case
            when staging_count > fact_count * 1.05 then staging_count - fact_count
            else 0
        end as violation_count
    from (
        select
            count(*) as staging_count
        from {{ ref('int_sales_unified') }}
        where sale_date >= current_date() - interval '7 days'
    ) s
    cross join (
        select
            count(*) as fact_count
        from {{ ref('fact_sales') }} f
        inner join {{ ref('dim_date') }} d on f.date_key = d.date_key
        where d.date_value >= current_date() - interval '7 days'
    ) f

    union all

    -- Rule 13: Critical business metrics should be within expected ranges
    select
        'CROSS_LAYER' as layer,
        'METRIC_OUT_OF_RANGE' as rule_type,
        'daily_sales_volume' as model_name,
        case
            when daily_sales < 1000 or daily_sales > 1000000 then 1
            else 0
        end as violation_count
    from (
        select sum(net_sales_amount) as daily_sales
        from {{ ref('fact_sales') }} f
        inner join {{ ref('dim_date') }} d on f.date_key = d.date_key
        where d.date_value = current_date() - interval '1 day'
    ) daily_metrics
),

all_business_rule_violations as (
    select * from staging_business_rules
    union all
    select * from intermediate_business_rules
    union all
    select * from marts_business_rules
    union all
    select * from cross_layer_validation
)

-- This test fails if any business rule violations are found
select *
from all_business_rule_violations
where violation_count > 0
