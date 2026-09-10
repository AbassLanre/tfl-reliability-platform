# Session Handoff — TfL Reliability Platform

Paste/attach this into a new Claude session along with the build plan PDF
(tfl_reliability_platform_build_plan_v1_2.pdf). Invoke the
data-engineering-mentor skill first.

## Who I am / how to work with me

- PercyAbs. Comfortable-ish (can need some help sometimes): Python, SQL, Postgres, dbt, Airflow, Docker, Git.
  Newer to: Snowflake, Kafka (learned Week 2). Spark is BRAND NEW as of
  Week 3 — first time ever with the framework. Plain English, analogies,
  one idea per message.
- Agreed work style: "mentor guides, Percy types/runs everything himself."
  One small step at a time; verify current facts against live docs and say
  what's verified vs. from memory. Do NOT pack multiple actions into one
  dense instruction — unpack them, not complex english.
- Hints before answers on learning exercises; escalate to partial code with
  TODO(you) gaps when stuck. Two swings, then the answer with reasoning.
- Every verification has a PREDICTED output; check the prediction, not just
  that it ran (lesson learned the hard way — see deduper bug below).
- Machine: Windows, PowerShell. Project folder:
  C:\Users\user\Documents\tfl-reliability-platform (connect it to the session).
- Keep NOTES.md entries for every bug/decision (Week 10 needs it). Percy
  keeps NOTES.md live DURING sessions, in his own words — this is working.

## Project status (as of 2026-09-09)

- Week 0 ✅  Week 1 ✅  Week 2 ✅  Week 3: Day 1 (concepts) ✅, Day 2 Step 1
  (skew measurement) ✅ committed 222aa0a. Day 2 Step 2 (bronze job):
  sub-steps 1-3 ✅ (console sink, full 20-column schema, quarantine split),
  sub-step 4 (Parquet sink) is NEXT. streaming/bronze_arrivals.py not yet
  committed — commit by filename, not `git add .` (line-ending churn).

## What was built in Week 2 (all working)

- **docker-compose.yml**: apache/kafka:4.3.0, KRaft single broker, named
  volume `kafka-data` mounted at /tmp/kraft-combined-logs (data survives
  down/up — roundtrip-tested). Volume ownership had to be chown'd to
  uid 1000 via throwaway alpine container. Image uses a FIXED default
  CLUSTER_ID, so no mismatch issue on recreate. NOTE: broker advertises
  localhost:9092 only — fine for local-venv Spark, would need an internal
  listener if Spark ever runs in a container (Week 7).
- **Topics** (created deliberately): tfl.arrivals (11 partitions — ceiling =
  one consumer per line's traffic, ~11 keys; hash lumpiness means partitions
  3 & 9 are permanently empty and 10 is hottest — expected, explained),
  tfl.line-status (1), tfl.disruptions (1). Replication factor 1 (one broker).
- **docs/schema.md v1**: envelope (schema_version, event_type, ingested_at,
  event_ts) + per-topic payload tables with type/required/source columns.
  Decisions recorded: snake_case mapped from TfL camelCase at producer;
  event_ts per topic = arrivals→timestamp, line-status→ingested_at (TfL's
  modified/created are stale or .NET-default garbage), disruptions→
  lastUpdate-or-ingested_at; grain of line-status = ONE STATUS PER LINE
  (explode TfL's nested lineStatuses list); scope = 11 tube lines, Elizabeth
  line documented as future expansion (mode_name field kept for this);
  required field = raw["x"] (KeyError enforcement), optional = raw.get("x").
- **ingestion/producer/** package (venv: confluent-kafka==2.15.0 pinned —
  verified live June 2026 release, kafka-python-ng is stale; python-dotenv;
  requests):
  - config.py — env-var config, .env via dotenv (gitignored), fail-fast
    RuntimeError if TFL_APP_KEY missing. TUBE_LINES list (11), poll
    intervals ARRIVALS_POLL_S=30, STATUS_POLL_S=60, DISRUPTION_POLL_S=60.
  - tfl_client.py — requests.Session, timeout=10 always, retry ONLY
    transient {429,5xx,network} with exponential backoff 1/2/4s, raise
    immediately on 4xx (our fault). Endpoints: /Line/{id}/Arrivals,
    /Line/Mode/tube/Status, /Line/{id}/Disruption (per line ON PURPOSE —
    disruption payload has no line field; the call IS the attribution;
    batching lines breaks attribution — bug found and fixed).
  - transform.py — pure functions (tested against saved samples in
    docs/sample_payloads/): build_arrival (1 dict), build_line_status
    (returns LIST — exploded grain), build_disruptions(raw, line_id) —
    context passed in by caller.
  - dedupe.py — sha256 fingerprint excluding volatile fields (ingested_at,
    event_ts); Deduper with bounded set (crude clear() at cap — acceptable
    because pipeline is at-least-once anyway). Applied to line-status and
    disruptions ONLY; arrivals never repeat byte-identically
    (time_to_station ticks), so no dedupe there — defended in README (TODO:
    actually write that README section, plus the no-Avro/Schema-Registry
    defence the plan requires).
  - main.py — due-times scheduling (time.monotonic), one loop no threads,
    p.poll(1) heartbeat services delivery callbacks, per-line try/except
    containment, KeyboardInterrupt → flush(10). Dedupers created ONCE in
    main() and passed in (the bug: creating them inside poll functions =
    new empty memory every cycle = zero skips; logs showed skipped 0 and
    the failed prediction went unnoticed for a run — twin lesson recorded).
  - Run with: python -m ingestion.producer.main  (from repo root, venv active)
  - Verified working: ~3,450 arrivals per 30s cycle, 14 statuses + 7-8
    disruptions per 60s cycle first poll, then produced 0/skipped 14 and
    0/8 on quiet cycles. Consumed messages match schema v1 exactly.
- Observed and understood: event-time skew (TfL event_ts ~43s older than
  ingested_at) — this is the Week 3 watermark material. THREE junk smoke-test
  messages exist in tfl.arrivals (event_type="smoke_test"; earlier notes said
  two — Spark groupBy on 2026-09-07 counted 3) — deliberate,
  consumers filter on event_type.

## Environment

- Python 3.13 venv at .venv/ (project libs only; standalone CLIs as
  binaries — snow CLI 3.23.0 is a Windows MSI).
- **Java: JDK 25.0.4.1** at C:\Program Files\Java\jdk-25.0.4.1, JAVA_HOME
  set (upgraded from 25.0.2 on 2026-09-04 because Spark 4.2.0 docs say
  "Java 25 prior to 25.0.3 support is deprecated").
- **pyspark==4.2.0 installed in .venv** (2026-09-04). Smoke test passed:
  SparkSession local[2], s.range(5).count() == 5, s.version == 4.2.0.
  NOT YET added to requirements.txt.
- **winutils + hadoop.dll BOTH FIXED (2026-09-08).** HADOOP_HOME=C:\hadoop.
  C:\hadoop\bin is on the USER PATH as of 2026-09-08 (set via
  [Environment]::SetEnvironmentVariable, verified in a fresh terminal). The
  2026-09-07 note claiming PATH was set was WRONG — setx never took; only
  HADOOP_HOME was set. Why it mattered: winutils.exe is found via
  HADOOP_HOME, but hadoop.dll is loaded via System.loadLibrary => PATH.
  So chmod worked and the dll silently didn't load. Batch reads survive
  without the dll; the FIRST streaming query died with
  UnsatisfiedLinkError NativeIO$Windows.access0 (checkpoint dir listing).
  "Harmless WARN turned fatal on a new code path" — recorded in NOTES.md.
  Verify with: NativeCodeLoader.isNativeCodeLoaded() via s._jvm == True,
  and no `WARN NativeCodeLoader` on startup.
  Superseded text kept for history: winutils.exe + hadoop.dll from
  kontext-tech/winutils `hadoop-3.4.0-win10-x64/bin` (newest public build;
  cdarlint stops at 3.3.6). PySpark 4.2.0 bundles hadoop-client 3.5.0, so
  it's a version mismatch: winutils.exe works, hadoop.dll does NOT load
  (`WARN NativeCodeLoader ... using builtin-java classes` persists — harmless).
  Why it bit early: `spark.jars.packages` → SparkContext.addFile → chmod →
  winutils, so the WARN became a fatal ERROR the first time a package was
  configured, not at the first Parquet write as predicted. Recorded in NOTES.md.
- **Kafka connector JARs cached** in C:\Users\user\.ivy2.5.2 — second run
  prints "0 artifacts copied, 11 already retrieved". Pulled kafka-clients 3.9.2.
- **PYSPARK_PYTHON=python** set via setx (Windows has no `python3`; PySpark
  warned "Missing Python executable 'python3'"). Not yet confirmed the
  warning is gone — check on next run.
- **spark.sql.session.timeZone=UTC** set on the SparkSession builder. Spark
  displayed 17:02 UTC data as 18:02 (Windows TZ Europe/London, BST). Stored
  values were correct; only display shifted. Pin UTC in every Spark script.
- **Shutdown noise**: `ERROR ShutdownHookManager: Exception while deleting
  Spark temp dir ... snappy-java.jar` on every exit — Windows file lock,
  cosmetic. %TEMP%\spark-* may accumulate; delete by hand if needed.
- **Git line endings**: `git status` shows 16 files modified with equal
  insertions/deletions (~28.6k each) = CRLF/LF churn, not real changes.
  `core.autocrlf` is UNSET. Do NOT `git add .` until fixed; add by filename.
  Fix (time-boxed, next session start): decide `core.autocrlf=true` or a
  `.gitattributes` with `* text=auto`, then renormalise
  (`git add --renormalize .`) in ONE dedicated commit. Also: commit 222aa0a's
  message starts with a stray `"` — PowerShell quoting; cosmetic.
- **requirements.txt is UTF-16 LE with BOM and CRLF** (PowerShell
  `pip freeze >` default) and is a full 93-line freeze including dbt's
  transitive deps. Committed that way. Will break `pip install -r` on
  Linux (Week 8 GitHub Actions). Fix pending: re-export as UTF-8
  (`pip freeze | Out-File -Encoding utf8`) or, better, curate a short
  top-level list. Add pyspark==4.2.0 when doing this.
- Docker Desktop; remember habit: `docker compose down` the fintech Airflow
  stack before Kafka/Spark sessions.
- Kafka CLI tools: docker exec -it kafka /opt/kafka/bin/<tool>.sh
  --bootstrap-server localhost:9092
- TfL app key in .env at repo root (TFL_APP_KEY=...), gitignored.

## Snowflake status (IMPORTANT)

- The $400 trial EXPIRED (~mid-Aug). Decision made with mentor: do NOT
  create a new account until Week 4 starts (Weeks 2-3 are all-local; don't
  burn the 30-day clock). When Week 4 starts: new trial, Standard / AWS /
  **eu-west-2** (matches Week 7 S3 region), and FIRST write
  snowflake/bootstrap.sql that rebuilds everything from code (warehouse
  TFL_DEV_WH X-Small AUTO_SUSPEND=60, DB TFL_DEV, RAW + STAGING schemas,
  CSV file format, stage, TUBE_LINES load, ALTER USER SET RSA_PUBLIC_KEY).
  RSA keys survive at C:\Users\user\.snowflake\keys\ (outside repo);
  config.toml and dbt/profiles.yml each need only the account field
  changed. Framing: "trial expired → I automated the environment rebuild"
  = interview story. (Honesty note discussed: trial-cycling is a ToS grey
  area; paid on-demand would cost pence at this usage — Percy's call.)

## Week 3 — IN PROGRESS (Spark Structured Streaming, local)

### Decisions made (2026-09-04)

- **Runtime: PySpark in the local Windows .venv**, not Docker, not WSL2.
  Reason: Week 3's difficulty is event-time semantics, not container
  networking; Kafka is already reachable on localhost:9092; pytest and IDE
  work natively. Containerising Spark is deferred to Week 7 where the plan
  already puts it. Fallback if Windows fights back: Spark in Docker with an
  internal Kafka listener.
- **Watermark delay for arrivals is a MEASURED decision, not a guess.**
  Observed skew ~43s is a typical value, not the tail; TfL 5xx = TfL under
  stress = staler predictions = bigger skew, exactly during the incidents
  the project cares about. Trade-off is lopsided (generous watermark costs
  seconds of latency; stingy one silently drops data). Plan: measure
  p99/max of (ingested_at - event_ts) over collected data, set watermark
  comfortably above p99, record the number in README.

### Verified live against spark.apache.org (2026-09-04) — not from memory

- Latest Spark: **4.2.0** (released 2026-07-14). Also 4.1.3, 4.0.4, 3.5.9.
- Kafka connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`
  (Scala 2.13 — Spark 4 dropped 2.12). Pulls kafka-clients + commons-pool2.
  For Python: pass via `--packages` or `spark.jars.packages` config.
- PySpark 4.2.0: Python 3.10+, Java 17/21/25 (25 needs >= 25.0.3).
- Kafka source DataFrame columns: key (binary), value (binary), topic,
  partition, offset, timestamp, timestampType, headers. Options:
  startingOffsets (default "latest" streaming / "earliest" batch),
  failOnDataLoss (default true), maxOffsetsPerTrigger.
- `dropDuplicatesWithinWatermark` is documented in 4.2.0. Distinction:
  plain dropDuplicates + watermark needs the event-time column IN the
  dedup key; dropDuplicatesWithinWatermark dedupes on the id alone and
  lets the watermark bound state.
- Watermark semantics (docs quote): window ending at T keeps state
  "until (max event time seen by the engine - late threshold **>** T)".
  STRICTLY GREATER — at exactly-equal the window is still open. Percy
  caught this boundary; mentor's shorthand had said >=.
- Guarantee is one-directional: data less than the delay late is
  guaranteed aggregated; data later than that "may or may not" be dropped.
- Output modes for watermarked windowed aggregation: Append, Update,
  Complete. Append emits a window only once finalised (after watermark
  passes it).
- The Structured Streaming guide was split into sub-pages in Spark 4.0:
  https://spark.apache.org/docs/latest/streaming/ — the old single-page
  URL is a redirect stub. Kafka guide:
  https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html

### Day 1 (concepts) — DONE, oral exam passed

Covered: event time vs processing time; watermarks; tumbling vs sliding
windows; micro-batch model. All taught via postcards analogy, tied to
Percy's own 43s skew observation. Oral exam (6 questions, no notes)
passed; patches applied and already reflected in Percy's NOTES.md:
- Q2 needed the WHY (bounded state / memory) before the mechanism.
- Q4 reason for tumbling: non-overlapping windows sum cleanly into the
  hourly Week 5 marts; sliding would double-count.
- Q6 terminology: when data stops, watermark freezes and windows stay
  OPEN (unfinalised), not closed.

Findings from Day 1 worth carrying forward:
- **Per-topic event-time quality differs**: arrivals has true event time
  from TfL; line-status has NONE (event_ts == ingested_at by Week 2
  decision) so a watermark there does no real work; disruptions is
  lastUpdate-or-fallback. Watermarks only matter on arrivals.
- Three clocks in play: payload event_ts (event time), payload ingested_at
  (processing time), Kafka record `timestamp` column (also processing
  time — Spark hands it over for free, don't confuse it with event_ts).
- **OPEN DESIGN QUESTION for Week 9**: session-based collection means the
  LAST window of every session never finalises in Append mode — no later
  event arrives to push the watermark past it until the next session.
  Spotted in Week 3; decide in silver-job design (output mode choice,
  graceful drain, or document as expected). Do not solve yet.
- Percy's interview sentences (his words, refined): "event-time processing
  is reproducible on replay, processing-time is not"; "Structured
  Streaming is micro-batch by default, latency bounded by the trigger
  interval; Flink for sub-second."

### Day 2 Step 1 — DONE (2026-09-07): skew measured, watermark decided

- **streaming/measure_skew.py** (committed 222aa0a). Batch `spark.read
  .format("kafka")` on tfl.arrivals (batch default startingOffsets=earliest),
  `CAST(value AS STRING)`, `from_json` with a 3-field StructType
  (event_type string, event_ts timestamp, ingested_at timestamp — from_json
  ignores unlisted keys), filter event_type=="arrival",
  skew_s = ingested_at.cast(double) - event_ts.cast(double), then
  count/min/avg/percentile(0.5, 0.99)/max. Also groupBy(event_type) as a
  permanent sanity check.
- **Results** over 1,547,523 arrivals (~3.7 h of 26 Aug evening, no
  incidents): min 0.59 s | p50 4.5 s | avg 15.6 s | p99 64.3 s | max 80.8 s.
  Count matched broker offsets exactly (1,547,526 − 3 smoke_test). Zero
  parse failures.
- **Prediction miss recorded**: Week 2's "~43 s skew" was one glance that
  landed in the tail; the median is 4.5 s. Explanation: TfL regenerates
  predictions often, the 30 s poll lands at random points in TfL's cycle,
  so most rows are fresh with a long right tail.
- **Watermark decision: 2 minutes** on arrivals (above max, not just p99;
  generous side is cheap — seconds of latency — stingy side silently drops
  rows during the incidents the project measures). Two caveats written in
  NOTES.md and to go in README: sample has no incident period; revisit after
  a week of bronze data on disk.
- Parsing facts learned: TfL event_ts has 7 fractional digits
  (`...33.9495161Z`, .NET ticks); Spark TimestampType truncates to 6
  silently, no nulls. ingested_at is Python isoformat `+00:00`. Both parse
  via TimestampType inside from_json without a timestampFormat option.
- Teaching note for next mentor: Percy hit a wall when Spark vocabulary
  (binary column, from_json, StructType) was introduced as hints. What
  worked: map every Spark idea to a pandas equivalent first (value = one
  JSON string per cell; from_json = json_normalize with a declared column
  list; withColumn = df["x"] = ...; agg = groupby with no group), then give
  the code with line-by-line comments. Keep doing that for readStream.

### Day 2 Step 2 — IN PROGRESS (2026-09-08/09): sub-steps 1-3 done

**Kafka data loss and retention decision (2026-09-08).** The 26 Aug backlog
(1,547,526 msgs) was GONE: kafka-get-offsets earliest == latest on all 11
partitions. Cause: default topic retention 7 days (broker
log.retention.hours=168); data was 13 days old. It had survived to 7 Sep
(measure_skew read it) — mentor's belief, unverified: single active
segment per partition, lazily rolled; broker restart on 8 Sep triggered
roll + delete. Percy could check `docker logs kafka | Select-String
"tfl.arrivals-4" | Select-String "Rolled|Deleted"` if curious. Lesson
(Percy's words in NOTES.md): Kafka is a buffer not a store; bronze exists
to copy out before retention. DECISION: retention.ms=2592000000 (30 days)
applied to tfl.arrivals via kafka-configs.sh --alter, read back with
--describe. TODO(you): confirm it was also applied to tfl.line-status and
tfl.disruptions (mentor asked; not confirmed in chat). Week 2's
"survives down/up" test covered the volume, not retention — different
failure. 1,547,523 is DEAD as a success number.

**New backlog (2026-09-08 19:40 → ~23:30 UTC producer run).**
kafka-console-consumer --from-beginning counted **1,604,979** messages on
tfl.arrivals = **1,604,977 arrivals + 2 deliberate junk** (`this is not
json` and `{"hello": "world"}`, sent via kafka-console-producer, unkeyed,
so on arbitrary partitions). These are the Day 2 "done when" numbers:
bronze Parquet count == 1,604,977; quarantine text count == 2.

**streaming/bronze_arrivals.py — current state (uncommitted):**
- SparkSession builder copied from measure_skew (packages + UTC).
- readStream kafka, subscribe tfl.arrivals, startingOffsets=earliest,
  maxOffsetsPerTrigger=20000.
- text = value cast string as value_str. arrival_schema = full 20-field
  StructType from docs/schema.md: envelope (schema_version Integer,
  event_type String, event_ts Timestamp, ingested_at Timestamp) + 16
  payload fields (time_to_station Integer, rest String). Verified: 20
  columns, ints unquoted, vehicle_id "071" keeps leading zero (string!),
  expected_arrival stays string in bronze (silver casts).
- parsed = text.select(value_str, from_json(...).alias("j"))  — BOTH
  columns kept (first attempt selected only j → AnalysisException
  UNRESOLVED_COLUMN value_str; Percy learned to read the plan tree
  bottom-up: the Project node for `parsed` listed only `j`).
- is_bad = j.event_type.isNull() | j.event_ts.isNull()  (required-field
  test, deliberately NOT j.isNull(): mentor believes Spark 3/4 PERMISSIVE
  from_json returns a struct of nulls for corrupt input, not a null
  struct; required-field test also catches valid-JSON-wrong-shape).
  good = parsed.filter(~is_bad).select("j.*"); bad = parsed.filter(is_bad)
  .select("value_str").
- Two writeStreams, both console for now, queryName bronze_good /
  bronze_quarantine, trigger 10 s, spark.streams.awaitAnyTermination().
- Verified run 1: good = 20-col table; quarantine = empty value_str table.
  Quarantine query printed an EMPTY Batch 1 → console sink prints
  zero-row batches whenever the source had offsets (empty batch ≠ no
  batch). Two queries = two Kafka consumers, two reads, two checkpoints.
- NOT YET PROVEN: that Spark routed the 2 junk rows to quarantine (they
  were at the tail of a 60+ batch drain; scrollback is not verification).
  Proof moves to sub-step 4: spark.read.text(quarantine).count() == 2.

**Observations recorded in NOTES.md (2026-09-08):**
- Batch 0 "falling behind" WARN (15.5 s vs 10 s trigger) once = fine;
  recurring = sizing problem.
- Batch rhythm: one batch per ~30 s producer cycle, not per 10 s trigger.
  Trigger is a ceiling on latency, not a metronome. Percy's oral check
  passed (5 s trigger → every 6th trigger finds data; 19:41 row lands in
  19:40-19:45 window when watermark is 19:42 because the WINDOW is open).
- Log timestamps are BST (Windows clock); data is UTC (session tz). Same
  instant, one hour apart on screen.
- **UNEXPLAINED, do not chase yet:** first 20 rows of every batch (one
  line — central — same partition) show identical event_ts and a steady
  ~48 s skew (19:40:01→19:40:49, 19:42:33→19:43:21, ...). Last week's
  p50 was 4.5 s across all lines. Revisit when silver computes skew per
  line. Sits uncomfortably close to the 2-min watermark if incidents
  stretch it.
- Free consistency check: expected_arrival == event_ts + time_to_station
  exactly (19:40:01 + 65 s = 19:41:06). Confirms event_ts is TfL's
  prediction-generation time. Candidate silver test.
- Watermark is NOT in bronze at all (no grouping → nothing to close).
  It enters at silver with withWatermark("event_ts", "2 minutes").
  "Bronze keeps everything so silver can afford to be strict."

**Teaching notes for next mentor (what worked on 2026-09-08):**
- Hints-first still works for Percy on things he has a pattern for
  (StructType extension: one swing, clean). Give code with line comments
  for genuinely new Spark surface (writeStream, awaitAnyTermination).
- He reads plan trees now — point him at the plan, not the traceback.
- He caught that the trigger/producer arithmetic was "every 6th", so
  numeric oral checks are worth doing.
- Late-evening sessions: he asked for a recap of "which seconds are
  which" — the five-numbers table (30 s poll, 10 s trigger, 15.5 s batch
  duration, 48 s skew, 2 min watermark) landed well; reuse it.

### NEXT: sub-step 4 (Parquet sink), then sub-step 5 (kill/restart)

Start here tomorrow. Pre-flight: docker ps (kafka up), venv active, fresh
terminal (PATH has C:\hadoop\bin), producer NOT needed (1.6 M backlog).

4. **Parquet sink.** Before the first run: add `data/` to .gitignore.
   good → format("parquet"), option("path","data/bronze/arrivals"),
   option("checkpointLocation","data/checkpoints/bronze_arrivals"),
   partitionBy("date","hour") where date/hour are derived from
   **ingested_at** (bronze = as landed; event-time partitioning belongs to
   silver — mentor's lean, Percy decides and records). bad →
   format("text"), option("path","data/quarantine/arrivals"),
   option("checkpointLocation","data/checkpoints/bronze_quarantine").
   Each query MUST have its own checkpoint dir. Predicted: folders
   data/bronze/arrivals/date=2026-09-08/hour=19/ ... hour=23/ with
   part-*.snappy.parquet; drain takes ~81 batches × ~10 s ≈ 15 min per
   query. After drain: spark.read.parquet("data/bronze/arrivals").count()
   == 1,604,977 and spark.read.text("data/quarantine/arrivals").count()
   == 2 (this is also the deferred proof of sub-step 3). Watch for
   "falling behind" WARNs recurring — Parquet writes are heavier than
   console.
5. **Kill/restart drill (Day 2 done-when).** Ctrl+C mid-drain (say after
   Batch 20), restart, confirm it resumes from the checkpoint (Batch 21,
   not Batch 0), let it finish, counts still exactly 1,604,977 and 2 —
   no duplicates, no gaps. Note: the `_spark_metadata` folder in the
   Parquet path is what makes the file sink exactly-once on replay;
   spark.read.parquet honours it. Early rehearsal of Day 5.

Original sub-step plan (kept for reference):

Goal of bronze: land every tfl.arrivals message as-is (all schema.md
fields, not just 3) into Parquet on local disk, continuously, with
unparseable messages diverted to a quarantine folder instead of being
silently dropped. Bronze is "raw as landed"; no dedupe, no windows yet.

**Plain-English framing to open with**: readStream is the same DataFrame
as spark.read, except Spark re-runs the whole recipe every trigger on
only the new Kafka offsets (micro-batch loop from Day 1). Everything
Percy wrote in measure_skew.py (cast, from_json, select) is reused
verbatim; only `read`→`readStream` and the sink change.

Sub-steps (one at a time, prediction before each run):

1. **Console sink first**, file `streaming/bronze_arrivals.py`. Copy the
   SparkSession builder from measure_skew.py (packages + UTC). `readStream`
   from tfl.arrivals with `startingOffsets=earliest` (streaming default is
   latest — say this out loud) and `maxOffsetsPerTrigger=20000` so the 1.5 M
   backlog arrives in bounded batches. Reuse the 3-field schema for now.
   `writeStream.format("console").outputMode("append")
   .trigger(processingTime="10 seconds").start()` then `awaitTermination()`.
   Predicted: `Batch: 0` header, ~20k rows behind a 20-row table, then
   `Batch: 1`, ... every ~10 s; Ctrl+C stops it. This is Day 1's micro-batch
   loop made visible.
2. **Full schema**: extend the StructType to every tfl.arrivals field in
   docs/schema.md (types: time_to_station int, the rest string, the two
   timestamps TimestampType). Predicted: same batches, ~20 columns.
3. **Quarantine split**: keep `value_str` alongside the parsed struct.
   Rows where the struct is null (from_json failed) → quarantine sink as
   text; rows where it parsed → main sink. Design point to teach: one
   readStream, two writeStreams from the same parsed DataFrame, each with
   its own checkpoint. Predicted: quarantine gets 0 rows on this data
   (measure_skew proved zero parse failures) — then feed one deliberately
   broken message via kafka-console-producer to prove the path works.
4. **Parquet sink**: `format("parquet")`, `option("path",
   "data/bronze/arrivals")`, `option("checkpointLocation",
   "data/checkpoints/bronze_arrivals")`, `partitionBy("date","hour")`.
   Partition columns derived from **ingested_at** (bronze = as landed;
   event-time partitioning belongs to silver — discuss, mentor's lean, Percy
   decides). Add `data/` to .gitignore BEFORE the first run. winutils is now
   in place so this should work first time; if not, the real error goes in
   NOTES.md. Predicted: folders `data/bronze/arrivals/date=2026-08-26/hour=17/`
   with part-*.snappy.parquet files; `spark.read.parquet(...).count()`
   equals 1,547,523 after the backlog drains.
5. Day 2 "done when": kill the job mid-backlog, restart, confirm no
   duplicates and no gaps via count == 1,547,523 (checkpoint working).
   That doubles as an early rehearsal of Day 5's checkpoint drill.

Then Days 3-4 silver (dropDuplicatesWithinWatermark on composite key;
expected vs actual arrival delay per line+station, 5-min tumbling windows,
withWatermark("event_ts", "2 minutes")). Day 5 checkpoint + kill/restart
drill. pytest on static DataFrames throughout.

### Carry-over tasks (not blocking)

- README: no-dedupe-on-arrivals defence; no-Avro/Schema-Registry defence;
  skew measurement table + 2-minute watermark decision with caveats.
- Git line-ending renormalisation (see Environment).
- ~~Confirm PYSPARK_PYTHON warning is gone.~~ Confirmed gone 2026-09-08.
- requirements.txt: fix UTF-16 encoding, curate, add pyspark==4.2.0
  (still not added).
- terraform/placrholder.py is a typo'd filename (cosmetic; fix when
  Week 7 touches that folder).

## Mentor conventions to keep

- Verify current facts (versions, docs) live; say verified vs. from memory.
- Every week ends with the plan's "done when" checkpoint + oral exam
  without notes; predictions before experiments; failures become NOTES.md
  lines and interview stories.
- Time-box yak-shaves; always leave a working fallback path.
- One small step at a time; Percy types/runs everything himself. The
  handoff doc is the exception — mentor writes it, Percy reviews/commits.
- Update this handoff at each DAY boundary during Week 3, not just at the
  week boundary — the chat gets long once code and logs start.
