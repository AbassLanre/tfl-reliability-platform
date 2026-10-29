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
- [ ] NOTES.md + to_know.md + session_handoff.md updated; commit

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
- [ ] fix forward: ALTER ACCOUNT SET TIMEZONE='UTC' (add to bootstrap.sql), one-time UPDATE of existing snowflake_loaded_at
- [ ] reconciliation script: Kafka offsets vs RAW count per hour
- [ ] Day 5: done-when + oral exam; docs; commit
