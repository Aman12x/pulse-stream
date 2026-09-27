-- New-account funnel by signup day. Signup is approximated by the account's
-- first profile-record create (app.bsky.actor.profile), which a new account writes
-- when it is set up. Accounts that existed before the capture never create it
-- during the capture, so they are not counted as new. The first capture day is
-- excluded: it starts mid-day and can hold profile creates from accounts set up
-- just before the capture began.
with signups as (
    select account_id, min(event_date) as signup_date
    from {{ ref('stg_events') }}
    where collection = 'app.bsky.actor.profile' and operation = 'create'
    group by 1
),
bounds as (
    select min(event_date) as first_day, max(event_date) as last_day from {{ ref('stg_account_days') }}
),
activity as (
    select s.account_id, s.signup_date, a.event_date, a.posts, a.likes, a.follows
    from signups s join {{ ref('stg_account_days') }} a using (account_id)
),
received as (
    select e.subject_account_id as account_id, min(e.event_date) as first_received
    from {{ ref('stg_events') }} e
    where e.subject_account_id is not null and e.operation = 'create'
    group by 1
)
select
    s.signup_date,
    count(*) as signups,
    count(*) filter (where exists (select 1 from activity x where x.account_id = s.account_id and x.posts > 0
                                   and x.event_date <= s.signup_date + 6)) as posted_within_7d,
    count(*) filter (where r.first_received <= s.signup_date + 6) as received_engagement_within_7d,
    count(*) filter (where exists (select 1 from activity x where x.account_id = s.account_id
                                   and x.event_date = s.signup_date + 1)) as returned_day_1,
    case when s.signup_date + 7 <= b.last_day then
        count(*) filter (where exists (select 1 from activity x where x.account_id = s.account_id
                                       and x.event_date = s.signup_date + 7))
    end as returned_day_7,
    s.signup_date + 7 <= b.last_day as day_7_observable
from signups s
cross join bounds b
left join received r using (account_id)
where s.signup_date > b.first_day
group by s.signup_date, b.last_day
order by 1
