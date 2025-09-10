-- models/marts/dim_date.sql
-- Date dimension table with comprehensive calendar attributes

{{
  config(
    materialized='table',
    schema='marts',
    cluster_by=['date_value', 'year_number']
  )
}}

with date_spine as (
    select date_day from {{ ref('int_date_spine') }}
),

holidays as (
    -- Define common US holidays (this could come from a seed file)
    select date_value, holiday_name, true as is_federal
    from (
        values
        -- 2020 holidays
        ('2020-01-01', 'New Year''s Day', true),
        ('2020-07-04', 'Independence Day', true),
        ('2020-12-25', 'Christmas Day', true),
        -- 2021 holidays
        ('2021-01-01', 'New Year''s Day', true),
        ('2021-07-04', 'Independence Day', true),
        ('2021-12-25', 'Christmas Day', true),
        -- 2022 holidays
        ('2022-01-01', 'New Year''s Day', true),
        ('2022-07-04', 'Independence Day', true),
        ('2022-12-25', 'Christmas Day', true),
        -- 2023 holidays
        ('2023-01-01', 'New Year''s Day', true),
        ('2023-07-04', 'Independence Day', true),
        ('2023-12-25', 'Christmas Day', true),
        -- 2024 holidays
        ('2024-01-01', 'New Year''s Day', true),
        ('2024-07-04', 'Independence Day', true),
        ('2024-12-25', 'Christmas Day', true),
        -- 2025 holidays
        ('2025-01-01', 'New Year''s Day', true),
        ('2025-07-04', 'Independence Day', true),
        ('2025-12-25', 'Christmas Day', true)
    ) as h(date_value, holiday_name, is_federal)
),

date_attributes as (
    select
        date_day as date_value,

        -- Generate date key (YYYYMMDD format)
        cast(to_char(date_day, 'YYYYMMDD') as number(8,0)) as date_key,

        -- Basic date parts
        extract(dayofweek from date_day) as day_of_week,
        extract(day from date_day) as day_of_month,
        extract(dayofyear from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,
        extract(month from date_day) as month_number,
        extract(quarter from date_day) as quarter,
        extract(year from date_day) as year_number,

        -- Day name
        case extract(dayofweek from date_day)
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 0 then 'Saturday'
            else 'Saturday'
        end as day_name,

        -- Month names
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

        case extract(month from date_day)
            when 1 then 'Jan'
            when 2 then 'Feb'
            when 3 then 'Mar'
            when 4 then 'Apr'
            when 5 then 'May'
            when 6 then 'Jun'
            when 7 then 'Jul'
            when 8 then 'Aug'
            when 9 then 'Sep'
            when 10 then 'Oct'
            when 11 then 'Nov'
            when 12 then 'Dec'
        end as month_abbreviation,

        -- Quarter name
        'Q' || extract(quarter from date_day) as quarter_name,

        -- Weekend flag
        case when extract(dayofweek from date_day) in (0, 1) then true else false end as is_weekend,

        -- Week boundaries
        date_trunc('week', date_day) as week_start_date,
        date_trunc('week', date_day) + interval '6 days' as week_end_date,

        -- Fiscal year (assuming fiscal year starts in February)
        case
            when extract(month from date_day) >= 2
            then extract(year from date_day)
            else extract(year from date_day) - 1
        end as fiscal_year,

        -- Fiscal quarter
        case
            when extract(month from date_day) in (2, 3, 4) then 1
            when extract(month from date_day) in (5, 6, 7) then 2
            when extract(month from date_day) in (8, 9, 10) then 3
            else 4
        end as fiscal_quarter,

        -- Fiscal month
        case
            when extract(month from date_day) >= 2
            then extract(month from date_day) - 1
            else extract(month from date_day) + 11
        end as fiscal_month

    from date_spine
),

final as (
    select
        d.date_key,
        d.date_value,
        d.day_of_week,
        d.day_of_month,
        d.day_of_year,
        d.day_name,
        d.week_of_year,
        d.week_start_date,
        d.week_end_date,
        d.month_number,
        d.month_name,
        d.month_abbreviation,
        d.quarter,
        d.quarter_name,
        d.year_number,
        d.is_weekend,

        -- Holiday information
        case when h.date_value is not null then true else false end as is_holiday,
        h.holiday_name,

        -- Business calendar fields
        d.fiscal_year,
        d.fiscal_quarter,
        d.fiscal_month,

        -- Audit columns
        {{ get_current_timestamp() }} as created_at,
        {{ get_current_timestamp() }} as updated_at

    from date_attributes d
    left join holidays h on d.date_value = h.date_value::date
)

select * from final
