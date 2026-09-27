-- A funnel step can never count more accounts than signed up that day.
select * from {{ ref('signup_cohorts') }}
where posted_within_7d > signups or received_engagement_within_7d > signups
   or returned_day_1 > signups or coalesce(returned_day_7, 0) > signups
