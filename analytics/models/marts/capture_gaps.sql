-- Minutes where the capture recorded far less than usual: under 20% of the median
-- minute. The feed itself never goes quiet for a minute (it runs at hundreds of
-- events a second), so these are capture outages, not quiet periods. The partial
-- first and last minutes are excluded.
with minutes as (
    select date_trunc('minute', event_at) as minute, count(*) as events
    from {{ ref('stg_events') }}
    group by 1
),
trimmed as (
    select * from minutes
    where minute > (select min(minute) from minutes) and minute < (select max(minute) from minutes)
),
all_minutes as (
    select unnest(generate_series((select min(minute) from trimmed), (select max(minute) from trimmed), interval 1 minute)) as minute
),
median as (select median(events) as m from trimmed)
select a.minute, coalesce(t.events, 0) as events, (select m from median) as median_minute
from all_minutes a left join trimmed t using (minute)
where coalesce(t.events, 0) < 0.2 * (select m from median)
order by 1
