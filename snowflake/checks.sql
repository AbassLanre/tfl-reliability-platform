-- checks.sql: health checks for RAW.ARRIVALS.
-- Safe to run any time. Nothing in this file changes data.

-- 1. How many rows have landed?
SELECT COUNT(*) FROM TFL_DEV.RAW.ARRIVALS;

-- 2. Is the pipe alive? Want executionState RUNNING and pendingFileCount 0.
SELECT SYSTEM$PIPE_STATUS('TFL_DEV.RAW.ARRIVALS_PIPE');

-- 3. What did the pipe load in the last hour? Any file with an error?
SELECT file_name, status, row_count, row_parsed, first_error_message
FROM TABLE(TFL_DEV.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'TFL_DEV.RAW.ARRIVALS',
  START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP())));

-- 4. Can I read fields out of the blob? 5 sample rows.
SELECT raw:line_id::string          AS line_id,
       raw:event_ts::timestamp_ntz  AS event_ts,
       raw:time_to_station::int     AS tts,
       snowflake_loaded_at
FROM TFL_DEV.RAW.ARRIVALS LIMIT 5;

-- 5. Are both clocks on UTC? Want two identical timestamps.
SELECT CURRENT_TIMESTAMP(), SYSDATE();

-- 6. Latency per hour: seconds from "we received it" to "it is in Snowflake".
--    Change the date to the day you want to check.
SELECT DATE_TRUNC('hour', ingested_at)                               AS ingest_hour,
       COUNT(*)                                                       AS n_rows,
       ROUND(PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY latency_s)) AS p50_s,
       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_s)) AS p95_s,
       MIN(latency_s) AS min_s, MAX(latency_s) AS max_s
FROM (
  SELECT raw:ingested_at::timestamp_ntz AS ingested_at,
         DATEDIFF('second', raw:ingested_at::timestamp_ntz, snowflake_loaded_at) AS latency_s
  FROM TFL_DEV.RAW.ARRIVALS
  WHERE snowflake_loaded_at >= '2026-10-28'
)
GROUP BY 1 ORDER BY 1;