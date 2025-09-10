-- models/intermediate/int_date_spine.sql
-- Generate a complete date spine for the date dimension

{{
  config(
    materialized='table',
    schema='staging'
  )
}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2030-12-31' as date)"
) }}

-- Alternative implementation if dbt_utils is not available
/*
with date_spine as (
    select
        dateadd('day', row_number() over (order by null) - 1, '2020-01-01'::date) as date_day
    from table(generator(rowcount => 4018))  -- 11 years * ~365 days
)

select
    date_day,
    extract(year from date_day) as year_number,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day_number,
    extract(dayofweek from date_day) as day_of_week,
    extract(dayofyear from date_day) as day_of_year,
    extract(week from date_day) as week_of_year,
    extract(quarter from date_day) as quarter,

    -- Derived date attributes
    case extract(dayofweek from date_day)
        when 1 then 'Sunday'
        when 2 then 'Monday'
        when 3 then 'Tuesday'
        when 4 then 'Wednesday'
        when 5 then 'Thursday'
        when 6 then 'Friday'
        when 7 then 'Saturday'
    end as day_name,

    case extract(month from date_day)
        when 1 then 'January'
        when 2 then 'February'
        when 3 then 'March'
        when 4 then 'April'
        when 5 then 'May'
        when 6 then 'June'
        when 7 then 'July'
        when 8 then 'August'
        when 9 then 'September'
        when 10 then 'October'
        when 11 then 'November'
        when 12 then 'December'
    end as month_name,

    extract(dayofweek from date_day) in (1, 7) as is_weekend,

    -- Business calendar fields (fiscal year starts in February for example)
    case
        when extract(month from date_day) >= 2 then extract(year from date_day)
        else extract(year from date_day) - 1
    end as fiscal_year,

    case
        when extract(month from date_day) in (2, 3, 4) then 1
        when extract(month from date_day) in (5, 6, 7) then 2
        when extract(month from date_day) in (8, 9, 10) then 3
        else 4
    end as fiscal_quarter,

    -- Week start/end dates
    date_trunc('week', date_day) as week_start_date,
    date_trunc('week', date_day) + interval '6 days' as week_end_date

from date_spine
*/
