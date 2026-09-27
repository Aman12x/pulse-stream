-- DAU, WAU (trailing 7 days, including the day) and stickiness (DAU / WAU).
-- WAU is only reported once 7 full days of capture precede it; earlier values
-- would undercount.
with days as (
    select distinct event_date from {{ ref('stg_account_days') }}
),
bounds as (
    select min(event_date) as first_day from days
)
select
    d.event_date,
    (select count(*) from {{ ref('stg_account_days') }} a where a.event_date = d.event_date) as dau,
    case when d.event_date >= b.first_day + 6 then
        (select count(distinct a.account_id) from {{ ref('stg_account_days') }} a
         where a.event_date between d.event_date - 6 and d.event_date)
    end as wau,
    round(
        (select count(*) from {{ ref('stg_account_days') }} a where a.event_date = d.event_date)::double
        / nullif(case when d.event_date >= b.first_day + 6 then
            (select count(distinct a.account_id) from {{ ref('stg_account_days') }} a
             where a.event_date between d.event_date - 6 and d.event_date) end, 0), 4) as stickiness
from days d cross join bounds b
order by 1
