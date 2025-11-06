-- tests/singular/test_scd2_integrity.sql
-- Ensure SCD2 dimensions maintain proper versioning and date integrity

with product_scd2_issues as (
    -- Check for overlapping date ranges for the same product
    select
        'PRODUCT' as dimension_type,
        'OVERLAPPING_DATES' as issue_type,
        p1.product_id as business_key,
        count(*) as issue_count
    from {{ ref('int_product_scd2') }} p1
    inner join {{ ref('int_product_scd2') }} p2
        on p1.product_id = p2.product_id
        and p1.product_scd2_key != p2.product_scd2_key
        and p1.effective_date <= p2.expiration_date
        and p2.effective_date <= p1.expiration_date
    group by p1.product_id

    union all

    -- Check for gaps in date ranges
    select
        'PRODUCT' as dimension_type,
        'DATE_GAPS' as issue_type,
        p1.product_id as business_key,
        1 as issue_count
    from {{ ref('int_product_scd2') }} p1
    left join {{ ref('int_product_scd2') }} p2
        on p1.product_id = p2.product_id
        and p2.effective_date = p1.expiration_date + interval '1 day'
    where p1.expiration_date != '2099-12-31'  -- Not the current record
        and p2.product_scd2_key is null  -- No following record found

    union all

    -- Check for multiple current records
    select
        'PRODUCT' as dimension_type,
        'MULTIPLE_CURRENT' as issue_type,
        p.product_id as business_key,
        count(*) as issue_count
    from {{ ref('int_product_scd2') }} p
    where p.is_current = true
    group by p.product_id
    having count(*) > 1
),

store_scd2_issues as (
    -- Same checks for stores
    select
        'STORE' as dimension_type,
        'OVERLAPPING_DATES' as issue_type,
        s1.store_id as business_key,
        count(*) as issue_count
    from {{ ref('int_store_scd2') }} s1
    inner join {{ ref('int_store_scd2') }} s2
        on s1.store_id = s2.store_id
        and s1.store_scd2_key != s2.store_scd2_key
        and s1.effective_date <= s2.expiration_date
        and s2.effective_date <= s1.expiration_date
    group by s1.store_id

    union all

    select
        'STORE' as dimension_type,
        'DATE_GAPS' as issue_type,
        s1.store_id as business_key,
        1 as issue_count
    from {{ ref('int_store_scd2') }} s1
    left join {{ ref('int_store_scd2') }} s2
        on s1.store_id = s2.store_id
        and s2.effective_date = s1.expiration_date + interval '1 day'
    where s1.expiration_date != '2099-12-31'
        and s2.store_scd2_key is null

    union all

    select
        'STORE' as dimension_type,
        'MULTIPLE_CURRENT' as issue_type,
        s.store_id as business_key,
        count(*) as issue_count
    from {{ ref('int_store_scd2') }} s
    where s.is_current = true
    group by s.store_id
    having count(*) > 1
)

-- This test fails if there are any SCD2 integrity issues
select *
from product_scd2_issues
union all
select *
from store_scd2_issues
