# TASKS.md — Week 4 Day 3 part 2 (Snowflake landing)  [mentor-kept; Percy commits]

Goal: events flow Kafka -> Spark -> S3 -> Snowpipe -> RAW.ARRIVALS with no manual action,
and the count matches end to end.

Done when:
- [x] bootstrap.sql runs clean twice on the new account (2nd run created the pipe)
- [x] IAM handshake: LIST @stage == S3 object count (136 == 136)
- [x] S3 event notification -> Snowflake SQS (snowpipe-arrivals, prefix arrivals/, suffix .parquet)
- [x] backlog in RAW.ARRIVALS == 1,604,977 (via COPY INTO; 2nd COPY = 0 files)
- [x] step 5: snow sql (connection is named tfl; set-default) + dbt debug green
- [x] step 8: AUTO_INGEST proven 28 Oct: 19,977 new Kafka msgs -> RAW 1,624,954 (+19,977), no manual step
- [x] step 9: D20 = B (dbt unique test on a row fingerprint in Week 5; detect, do not silently dedupe)
- [ ] to_know.md + session_handoff.md updated by mentor 13 Nov (Percy: review, then commit all)

Key facts (verified this session):
- Account: BWQDSUN-PF94891, user PERCYABS1, region AWS_EU_WEST_2, student trial 120 days (started 16 Oct 2026)
- Snowflake IAM user: arn:aws:iam::042467473907:user/t61b2000-s ; role snowflake-tfl-s3-role in 388096320305
- SQS: arn:aws:sqs:eu-west-2:042467473907:sf-snowpipe-AIDAQTY2CHHZ7EEN3OFPO-E84wTMtuNJ20xFhAcT8sHA
- S3 arrivals/: 136 objects = 126 parquet + 10 _spark_metadata (mentor predicted 9: wrong)
- Public key file: C:\Users\user\.snowflake\keys\snowflake_key.pub

Lessons for NOTES.md (Percy's words):
- CREATE PIPE AUTO_INGEST validates bucket access at creation -> role must exist first
- IAM policy needs s3:GetBucketLocation too (4 perms, verified docs 16 Oct)
- ALTER PIPE REFRESH only sees files staged in the last 7 days (hard max) -> COPY INTO for backfills
- SYSTEM$VALIDATE_STORAGE_INTEGRATION READ test needs a real file name

Day 4 (28 Oct):
- [x] clocks: CURRENT_TIMESTAMP = America/Los_Angeles (account default), SYSDATE = UTC, 7 h apart; raw:ingested_at arrives as TIMESTAMP_NTZ
- [x] latency on 19,977 live rows (LA stamp undone): n 19,977 | min 512 s | p50 584 | p95 672 | max 675; all rows one Snowpipe load at 16:14:31 UTC
      CAVEAT: producer ran first, stream started after -> number includes test-order wait; real figure needs both running together (Week 9)
      mentor predicted p50 180-300 / p95 330-450: WRONG, forgot the serial test order
- [x] fix forward RUN in Snowflake 28 Oct: ALTER ACCOUNT SET TIMEZONE='UTC' + one-time UPDATE of snowflake_loaded_at (Percy: outputs as predicted)
- [x] bootstrap.sql: ALTER ACCOUNT SET TIMEZONE = 'UTC'; is line 1 (checked in file 13 Nov)
- [x] snowflake/checks.sql: 6 read-only checks, no UPDATE/ALTER/COPY, no CONVERT_TIMEZONE (checked in file 13 Nov)
- [x] reconciliation script: 28 Oct -> 19,977 == 19,977 OK; 9 Sep -> kafka 0 vs snowflake 1,452,804 MISMATCH (expected: Kafka retention)
      13 Nov VERIFIED: earliest offsets sum = 3,152,505 (8-9 Sep data deleted by 30-d retention, mentor prediction right); latest 3,172,482
      -> Kafka holds only the 19,977 msgs of 28 Oct; they expire ~27 Nov
      script = scripts/reconcile.py <YYYY-MM-DD>: Kafka offsets_for_times per hour vs RAW count by ingested_at hour; exit 1 on mismatch
      APIs checked in Percy's .venv (confluent-kafka 2.15.0 docstring, snowflake-connector 4.6.0 connection_name)
- [x] Day 5 done-when: stack runs with no manual step (28 Oct) + reconciliation matches (13 Nov)
- [x] Day 5 oral exam answered 13 Nov: Q5 pass | Q3, Q4 half | Q1, Q2 miss. Re-drill Q1 + Q2 same day: both PASS (Q1 thin on cost: per-file vs warehouse always on; Q2 left out 'what breaks without the external ID')
- [ ] README Week 4 section (D18, D19, D20, REFRESH lesson, latency with caveat, reconcile table)
- [ ] cosmetic: reconcile.py diff column width >8 -> >12

## Week 5 — dbt (started 13 Nov, new chat)
- [x] 5.1 raw.arrivals freshness (config: loaded_at_field + warn 30 min / error 2 h). ERROR STALE as predicted;
      max_loaded_at 2026-10-28T16:14:31 UTC. Bug on the way: `{count:30}` (no space) -> YAML read it as one key,
      dbt said "Nothing to do".
- [x] 5.2 tube_lines -> dbt seed (dbt/seeds/tube_lines.csv), stg_tube_lines uses ref(). INSERT 12 (mentor said 11:
      Elizabeth line row, kept as future expansion).
- [x] 5.3 stg_arrivals view: 20 fields out of the VARIANT (not 22: date/hour are S3 folder names, not in the files).
      Count 1,624,954 == RAW. Added row_fingerprint = md5(to_varchar(raw)).
- [x] 5.4 tests: 12 on stg_arrivals. D20 unique fingerprint FIRED: 4 duplicate rows, all piccadilly, 9 Sep,
      30 s apart (= poll interval), same load -> upstream TfL repeat, NOT a double-loaded file (that would be thousands).
      Tuned: warn_if >0, error_if >100. Result PASS=12 WARN=1.
- [x] 5.5 D21 = B: rebuild silver in dbt from RAW; Spark stays the real-time engine; Week 10 reconciles the two.
- [x] 6.1 int_arrival_predictions (table): Week 3 dedupe key (line_id, vehicle_id, naptan_id, event_ts), '000' removed,
      CTE + row_number + exclude(rn). 1,068,808 rows (~1.52 copies per prediction; mentor predicted 1.1-1.2M: a bit off).
      Bug: key used to_varchar(event_ts) = milliseconds only -> 55 sub-ms collisions; fixed with 'YYYY-MM-DD HH24:MI:SS.FF9'.
      unique + not_null PASS.
- [ ] 6.2 int_arrivals: one row per train reaching a station. MUST copy the Week 3 session-gap logic from
      streaming/silver_arrivals.py (mentor reads it first; do not guess).
- [ ] gitignore dbt/.user.yml
- [ ] README Week 4 section (still open)