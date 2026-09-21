# to_know.md — session memory for interview prep

One entry per session or day. What was built, what was decided, what broke,
and the sentence I would say in an interview. Written from NOTES.md and
docs/session_handoff.md; Percy reviews and corrects in his own words.
Mentor updates this at the end of every session (convention added by
Percy, 16 Sep 2026).

Problem statement (everything traces back to this):
"Which London Underground lines are actually reliable, by line, by station,
by time of day, and can we detect a disruption forming before TfL
officially declares it?"

---

## Week 0 — Setup (July 2026)

**Done:** TfL API key; AWS account with budget alerts (£2/£5/£10), IAM user
(never root), CLI, eu-west-2; public GitHub repo with the folder skeleton
(ingestion/ streaming/ dbt/ airflow/ terraform/ dashboard/ docs/) plus
NOTES.md and a stub README; Docker Desktop, Python venv, Terraform CLI.
Machine specs recorded: i7 9th gen, 16 GB RAM, 125 GB free.

**Interview sentence:** "I set the cloud budget alarms before I created a
single resource. The whole project's cloud footprint is designed to stay
under £5."

## Week 1 — Snowflake foundations (activated 13 Jul 2026)

**Done:** Standard edition trial on AWS eu-west-2 (same region as the
future S3 bucket: keeps the storage-integration handshake simple and
transfer cost zero). Warehouse TFL_DEV_WH X-Small, AUTO_SUSPEND = 60,
AUTO_RESUME, INITIALLY_SUSPENDED. Loaded tube_lines.csv two ways: UI, then
stage + PUT + COPY INTO. dbt connected via RSA key-pair auth (MFA blocks
password auth for dbt; generated keys with scripts/generate_snowflake_key.py,
keys live outside the repo in ~/.snowflake/keys/).

**Broke / learned:** snow CLI installed into the project venv downgraded
protobuf/click and broke dbt-core → CLI tools live outside the project venv
(standalone binary), project venv holds project libs only. Python 3.10
resolver meltdown → moved to 3.13. Config edited in the wrong location once
(resolution order). Locale warning cp1252 vs utf-8, fixed in config.
Trial spend: $3.50 after 9 days.

**Interview sentences:**
- "Snowflake separates storage from compute: data lives once in object
  storage; virtual warehouses are stateless compute clusters billed per
  second that suspend when idle. Postgres welds the two together on one
  machine, so they scale together whether you like it or not."
- "COPY INTO is idempotent per FILE, not per row: Snowflake keeps ~64 days
  of load metadata and skips files it has already loaded. Same data in a
  differently named file would load twice."
- "A stage is the loading dock, the table is the warehouse floor. An
  external stage is a signpost to my own S3: files stay put, which gives
  audit trail, disaster recovery and replay."
- Snowpipe: "S3 event → SQS → automatic COPY INTO. Serverless from my point
  of view, with load history per pipe."

**Status now:** trial EXPIRED mid-Aug. Deliberate decision: don't recreate
until Week 4; when I do, the rebuild is scripted in snowflake/bootstrap.sql.
Story: "the trial expired, so I automated the environment rebuild."

## Week 2 — Kafka + TfL producer (late Aug 2026)

**Done:**
- Kafka 4.3.0, KRaft (no ZooKeeper), single broker in Docker, named volume
  for the log dir (survives down/up; had to chown the volume to uid 1000).
- Topics: tfl.arrivals (11 partitions, keyed by line_id), tfl.line-status
  (1), tfl.disruptions (1). Replication factor 1 (one broker).
- docs/schema.md v1: envelope (schema_version, event_type, ingested_at,
  event_ts) + per-topic payload. snake_case at the producer. Line-status
  grain = ONE STATUS PER LINE (TfL's nested list is exploded).
- ingestion/producer/ package: config (fail-fast on missing key),
  tfl_client (timeout=10 always; retry ONLY 429/5xx/network with 1/2/4 s
  backoff; raise immediately on 4xx), transform (pure functions tested
  against saved sample payloads), dedupe (sha256 fingerprint excluding
  volatile fields, bounded set), main (one loop, due-times scheduling,
  per-line try/except, poll(1) heartbeat for delivery callbacks).
- Verified: ~3,450 arrivals per 30 s cycle; 14 statuses + 7–8 disruptions
  on first 60 s cycle, then produced 0 / skipped 14 on quiet cycles.

**Broke / learned:**
- Dedupers created inside the poll function = new empty memory every cycle
  = zero skips. The logs said "skipped 0" and I did not notice the failed
  prediction for a whole run. Twin lesson: create state once and pass it
  in; check the PREDICTION, not just that it ran.
- Disruption endpoint has no line field; the per-line call IS the
  attribution. Batching lines broke attribution. Fixed.
- Partition hash lumpiness: with 11 keys on 11 partitions, partitions 3
  and 9 are permanently empty and 10 is the hottest. Expected.
- Keyless console messages are sticky-batched onto one partition.

**Interview sentences:**
- "Kafka guarantees order WITHIN a partition, because a partition is an
  append-only log with one leader appending in arrival order. It does NOT
  guarantee order across partitions: a Victoria and a Jubilee message have
  no defined order relative to each other."
- "Within a consumer group each partition is served to exactly one
  consumer. If a consumer dies, another picks up its partitions from the
  last committed offset, and anything processed-but-not-committed is
  processed again: at-least-once delivery."
- "Dedupe is an optimisation; idempotent consumers are the correctness
  guarantee. My deduper is a Python set in RAM: restart the producer and
  the first poll re-sends everything. That is acceptable only because
  downstream already has to survive Kafka's own re-delivery."
- "Two timestamps in every message because Spark windows on event time,
  and late data would land in the wrong window on ingestion time."
- Why 11 partitions: "11 line keys = 11 is the most parallelism that could
  ever be useful; the ceiling costs nothing at this scale."
- Why no Avro / Schema Registry: single producer, single team, schema in
  docs/schema.md with schema_version in the envelope. Registry is the right
  answer once producers and consumers belong to different teams. (README
  section still to write.)

## Week 3 Day 1 — Structured Streaming concepts (4 Sep 2026)

**Decisions:** PySpark 4.2.0 in the local Windows venv (not Docker, not
WSL2): Week 3's difficulty is event-time semantics, not container
networking; containerising Spark is deferred to Week 7. Watermark delay is
a MEASURED decision, not a guess.

**Verified live against spark.apache.org (not memory):** Spark 4.2.0
(Jul 2026); connector spark-sql-kafka-0-10_2.13:4.2.0 (Spark 4 dropped
Scala 2.12); Java 25 needs >= 25.0.3; dropDuplicatesWithinWatermark exists;
watermark semantics use STRICTLY GREATER (window ending at T is dropped
when max event time − delay > T; at equality it is still open — I caught
the mentor's ">=" shorthand).

**Oral exam (6 questions, no notes): passed.** Patches: give the WHY
(bounded state / memory) before the mechanism; tumbling because
non-overlapping windows sum cleanly into hourly marts, sliding would
double-count; when data stops the watermark FREEZES and windows stay OPEN
(unfinalised), not closed.

**Interview sentences:**
- "Event time is when it happened (TfL's timestamp); processing time is
  when my pipeline saw it (ingested_at). Event-time processing is
  reproducible on replay; processing-time is not."
- "A watermark is newest event time seen minus a chosen delay. It is the
  deadline that lets Spark throw old window state away. Without it, state
  grows forever."
- "Structured Streaming is micro-batch by default; latency is bounded by
  the trigger interval. Flink is the answer for sub-second."
- Open design question for Week 9: in Append mode the LAST window of a
  session never finalises because no later event pushes the watermark past
  it. Spotted, parked, not solved yet.

## Week 3 Day 2 — skew measured, bronze job, kill/restart drill (7–11 Sep 2026)

**Step 1, measure_skew.py (batch over Kafka):** 1,547,523 arrivals from
26 Aug. skew = ingested_at − event_ts: min 0.59 s / p50 4.5 s / avg 15.6 s
/ p99 64.3 s / max 80.8 s. Prediction miss: Week 2's "~43 s skew" was one
glance in the tail; median is 4.5 s. Watermark decision: **2 minutes**,
above the max, because the trade-off is lopsided (generous = seconds of
latency; stingy = silently dropped rows during exactly the incidents the
project measures). Caveat: sample has no incident period.

**Broke / learned:**
- hadoop.dll never loaded because C:\hadoop\bin was not actually on PATH
  (setx never took; only HADOOP_HOME was set). winutils.exe is found via
  HADOOP_HOME but hadoop.dll via System.loadLibrary → PATH. Batch reads
  survived without it; the FIRST streaming query died
  (UnsatisfiedLinkError on checkpoint dir listing). "A warning about a
  missing component is only harmless until something needs the component."
- Kafka default retention is 7 days. The 26 Aug backlog was 13 days old and
  GONE on 8 Sep. "Kafka is a buffer, not a store; bronze exists to copy
  data out before retention deletes it." Set retention.ms = 30 days on
  tfl.arrivals (TODO: confirm on the other two topics). 1,547,523 is dead
  as a success number.
- Spark displayed UTC data in BST; pinned spark.sql.session.timeZone=UTC.
- Log timestamps are BST (Windows clock); data is UTC.

**Step 2, bronze_arrivals.py (streaming):** readStream from tfl.arrivals,
startingOffsets=earliest, maxOffsetsPerTrigger=20,000, full 20-field
schema from docs/schema.md, quarantine split on required-field-null test
(NOT struct-is-null: PERMISSIVE from_json gives a struct of nulls), two
writeStreams from one parsed DataFrame (Parquet partitioned by date/hour
from ingested_at; text quarantine), each with its OWN checkpoint.
Partition decision (mine): bronze partitions by ingested_at ("when did WE
receive it"); event_ts belongs in silver ("when did it HAPPEN").

**Kill/restart drill (Day 2 done-when):** killed at commit 35, restarted,
resumed at 36 (not 0), drained to 80 = 81 batches (1,604,979 / 20,000
rounds up to 81; prediction matched). Counts: 1,604,977 good Parquet rows,
2 quarantine rows (the two junk messages I injected). No gaps, no
duplicates.

**Interview sentences:**
- "Two ledgers, two jobs. The checkpoint records which Kafka offsets are
  done: it prevents GAPS and sets the restart point. _spark_metadata inside
  the Parquet path records which files each batch officially owns, written
  only after all files are on disk: it prevents DUPLICATES. Only Spark
  reads the second ledger."
- Extension for Week 7: "pandas, DuckDB, Snowflake COPY INTO and Snowpipe
  list the folder, not the catalogue, so orphan files from a killed batch
  WOULD count. That is the honest motivation for Iceberg/Delta: a catalogue
  every engine reads."
- "The trigger interval is a ceiling on latency, not a metronome: with a
  30 s producer and a 5 s trigger, every sixth trigger finds data."
- "Bronze keeps everything so silver can afford to be strict."

## Week 3 Day 3 — silver DESIGN by measurement (15–16 Sep 2026)

All batch over bronze Parquet, prediction before every run. Bronze now
holds 8 Sep AND 9 Sep data (producer re-run).

**Q1: what is one arrival?**
- groupBy(id): 16,822 distinct TfL ids, rows per id p50 90 / max 1,760.
  So `id` is stable across polls (my prediction, correct).
- The 1,760-row id: Uxbridge, vehicle_id "000" = TfL's placeholder for
  "train not identified", 3 different trains × 3 platforms per poll. TfL's
  id collapses all unidentified trains at a station into one id. "000" is
  1.06 % of rows.
- Non-000 rows grouped by (line, vehicle, naptan, event_ts): max 18 → +
  platform_name → 6 → + current_location → 4, avg 1.39. The rest were
  IDENTICAL predictions fetched in two consecutive polls (same event_ts,
  same countdown; only ingested_at differs). TfL does not regenerate every
  30 s; my producer re-fetches unchanged generations. This also EXPLAINS
  the 8 Sep "steady 48 s skew, identical event_ts" mystery.
- Three named sources of row multiplication: platform hedging, "000"
  collapse, re-fetched generations.

**Q1 decisions (in NOTES.md):** ROW → PREDICTION (line_id, vehicle_id,
naptan_id, event_ts) = dropDuplicatesWithinWatermark key → ARRIVAL
(line_id, vehicle_id, naptan_id). ingested_at deliberately OUT of the key
(it differs on every row; including it makes dedupe a no-op; same idea as
Week 2's fingerprint excluding volatile fields). platform_name OUT of the
key (1 s difference is noise at 5-minute grain). "000" rows excluded from
the delay metric and COUNTED as unattributable (a data-quality metric).

**Q2: what is an "actual" arrival?** TfL gives predictions only. My first
idea (countdown reaches 0) tested: per arrival min(time_to_station) over
16,928 arrivals: p50 **18 s**, p99 1,580 s. The countdown almost never hits
0 (30 s poll misses the last seconds); ~1 % vanish 25+ minutes out
(cancelled / lost / or truncated when I stopped the producer).
**Decisions:** completed = lowest time_to_station <= 60 s (two polls);
86.6 % of arrivals qualify. Not completed = counted, not measured.
**Delay = last expected_arrival − first expected_arrival** (prediction
drift). Uses only TfL's clocks → event-time only → reproducible on Kafka
replay; the alternative mixed in ingested_at (processing time).

**Interview sentences:**
- "There is no ground truth in the Arrivals feed. The moment a prediction
  DISAPPEARS is my proxy for the arrival. Delay means how far TfL's own
  forecast slipped between first and last sighting, not lateness against
  a timetable. That is a stated assumption in the README."
- "I did not assume the dedupe key, I measured it: TfL's id collapses
  unidentified trains, platform hedging fans one train into several rows,
  and consecutive polls re-fetch unchanged predictions. Each one has a
  named handling."
- "Session-end truncation is a known limitation: every train still on the
  board when collection stops looks like a lost train. It is the same
  problem as the last window never finalising in Append mode."

**Spark learned this day:** groupBy(...).count() returns a new DataFrame
with a column Spark names `count`; two-step summary pattern (one row per
entity, then agg across entities) ≈ pandas groupby().min().describe();
Spark recomputes a DataFrame on every action unless cached.

**Next:** Day 3 concepts, then silver_arrivals.py in batch. (Done 17-18 Sep, below.)

## Week 3 Day 3 concepts + Day 4 part 1 — silver in batch, two bugs (17–18 Sep 2026)

**Concepts:** state = what Spark must remember to finish a job (seen keys
for dedupe, open sums for windows). It must be bounded or RAM overflows.
The watermark is the one number that decides both what is too late to
accept and what is safe to forget. dropDuplicatesWithinWatermark bounds
state whether or not event_ts is in the key; plain dropDuplicates only if it
is. Append mode = a window is written once, when it finalises.

**Built:** `streaming/silver_arrivals.py`, batch over bronze Parquet.
Filter "000" (17,086 rows, 1.07 %) → cast expected_arrival → dedupe on
(line_id, vehicle_id, naptan_id, event_ts) → 1,056,017 predictions (ratio
1.504, matches Day 3) → arrivals → delay.

**Broke / learned (the good stuff):**
- expected_arrival == event_ts + time_to_station for 100 % of rows. It is a
  derived field. Free dbt test.
- dropDuplicates keeps an arbitrary row. Platform-hedging siblings carry
  different countdowns, so completed share moved 86.6 → 86.4 %. Measured,
  accepted, written down.
- **The arrival key had no time bound.** First delay result: max 24.1 hours,
  p50 1.8 hours, 9,633 "arrivals" delayed over an hour. Same train, same
  station, two different trips (across the two collection days AND within
  one afternoon, because a round trip is 1–2 h). Day 3's 16,928 was
  distinct (line, train, station), not arrivals.
  Fix: `session_window("event_ts", "10 minutes")` in the groupBy. New pile
  after 10 min of silence per key. 39,222 arrivals; impossible delays
  9,633 → 43. Gap sensitivity: 5 min 41,450 / 10 min 39,222 / 15 min 37,119.
  Chose 10, recorded the table, revisit with more data.
- **max(expected) − min(expected) is spread, not drift.** Never negative.
  Fixed with `min_by`/`max_by` on event_ts. Now: completed-only delay
  min −1,371 s, p50 41 s, p99 1,932 s. Completed share 73.6 % (fragments
  from split trips).
- Three mentor predictions were wrong today (one bronze folder; "a few
  dozen" cross-day merges; gap sensitivity "within a few percent"). The
  prediction habit is what caught all three.

**Interview sentences:**
- "My arrival key was right in space and unbounded in time. A 24-hour delay
  is impossible, so I traced it, fixed it with a session window, and the
  before/after is 9,633 impossible delays down to 43."
- "Session windows split by gaps and define identity; tumbling windows
  split by the clock and define reporting grain. Different rules, both are
  bounded by the watermark in streaming."
- "Dedupe is order-dependent. I measured what the arbitrary choice cost
  and documented it instead of pretending it was free."
- "The watermark does two jobs with one number: what is too late to accept,
  and therefore what is safe to forget."

**Spark learned:** session_window, min_by/max_by, `.cache()` when a
DataFrame is reused (Spark is lazy; a DataFrame is a recipe until cached),
`filter(col("flag"))` not `== True`.

**Next:** 5-minute tumbling windows per line/station; refactor into
functions; pytest; readStream switch with watermark + Append; Day 5 drill.
