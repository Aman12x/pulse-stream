-- Record creates per day and collection, and how many distinct accounts made them.
select
    event_date,
    collection,
    count(*) as creates,
    count(distinct account_id) as accounts
from {{ ref('stg_events') }}
where kind = 'commit' and operation = 'create'
group by 1, 2
order by 1, 3 desc
