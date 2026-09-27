-- One row per event. The archive is at-least-once (a range can be rewritten after
-- a crash between the file rename and the offset commit), so duplicates are
-- dropped here by event_id, keeping the earliest copy in the log.
select
    event_id,
    account_id,
    nullif(subject_account_id, '') as subject_account_id,
    kind,
    nullif(collection, '') as collection,
    nullif(operation, '') as operation,
    time_us,
    make_timestamp(time_us) as event_at,
    cast(make_timestamp(time_us) as date) as event_date
from read_parquet('{{ var("archive_glob") }}', hive_partitioning = true)
qualify row_number() over (partition by event_id order by kafka_partition, kafka_offset) = 1
