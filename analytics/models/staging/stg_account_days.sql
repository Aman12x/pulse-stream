-- An account is active on a day if it created, updated or deleted any record.
-- identity and account events are protocol housekeeping, not activity.
select
    account_id,
    event_date,
    count(*) as events,
    count(*) filter (where collection = 'app.bsky.feed.post' and operation = 'create') as posts,
    count(*) filter (where collection = 'app.bsky.feed.like' and operation = 'create') as likes,
    count(*) filter (where collection = 'app.bsky.feed.repost' and operation = 'create') as reposts,
    count(*) filter (where collection = 'app.bsky.graph.follow' and operation = 'create') as follows
from {{ ref('stg_events') }}
where kind = 'commit'
group by 1, 2
