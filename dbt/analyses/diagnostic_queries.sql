-- Diagnostic queries to identify root causes of test failures

-- ERROR 1: Customer segment values
SELECT 'ERROR 1: Customer Segment' as error_type,
       customer_segment,
       COUNT(*) as count
FROM {{ ref('dim_customer') }}
GROUP BY customer_segment
ORDER BY count DESC;

-- ERROR 2: Store type values
SELECT 'ERROR 2: Store Type' as error_type,
       store_type,
       COUNT(*) as count
FROM {{ ref('dim_store') }}
GROUP BY store_type
ORDER BY count DESC;

-- ERROR 3: Product SCD2 change_type values
SELECT 'ERROR 3: Change Type' as error_type,
       change_type,
       COUNT(*) as count
FROM {{ ref('int_product_scd2') }}
GROUP BY change_type
ORDER BY count DESC;

-- ERROR 4: Date spine row count
SELECT 'ERROR 4: Date Spine Count' as error_type,
       COUNT(*) as total_rows,
       MIN(date_day) as min_date,
       MAX(date_day) as max_date,
       DATEDIFF('day', MIN(date_day), MAX(date_day)) + 1 as day_span
FROM {{ ref('int_date_spine') }};

-- ERROR 5: Sales unified duplicates
SELECT 'ERROR 5: Sales Duplicates' as error_type,
       order_id,
       data_source,
       COUNT(*) as duplicate_count
FROM {{ ref('int_sales_unified') }}
GROUP BY order_id, data_source
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;
