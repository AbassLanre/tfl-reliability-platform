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

## Project status (as of 2026-09-23)

- Week 0 ✅  Week 1 ✅  Week 2 ✅  Week 3: Day 1 ✅, Day 2 ✅, Day 3 design ✅,
  Day 3 concepts ✅ (17 Sep: state / bounded state / watermark bounds it),
  Day 4 part 1 ✅ (17-18 Sep: silver_arrivals.py as BATCH over bronze,
  sub-steps 1-5 done, two design bugs found and fixed — see "Day 4 part 1
  DONE" below). Committed feb26b5.
  Day 4 part 2 ✅ (21-22 Sep: 5-min tumbling windows, refactor into six
  pure functions with __main__ guard, pytest x5 green). Commits b5b5672,
  bce0447, fbacbcc.
  Step 9 ✅ + Day 5 drill ✅ + Week 3 oral exam (3/2/1) ✅ (23 Sep: readStream
  on bronze Parquet, watermark + dropDuplicatesWithinWatermark, Append to
  Parquet data/silver/arrivals, 34,179 rows vs batch 39,222 = 12-minute
  cut-off measured; kill/restart resumed at 2, count identical). Commit
  5f79d0a. **WEEK 3 DONE.**
  24 Sep: Week 3 paperwork closed (exam re-drill passed, README D15/D16/D17,
  job 2 `streaming/window_reliability_batch.py` 12,546 windows, cosmetics,
  requirements.txt UTF-8 curated, .gitattributes). **WEEK 4 STARTED:** Day 1
  done (two S3 buckets, IAM write test). NEXT = see "NEXT (start here)":
  Week 4 Day 2 = Spark bronze sink to s3a://.
- README.md is no longer a stub (16 Sep): mentor-drafted decision log
  D1–D14 + numbers + local run steps, TODOs for Weeks 4–10. Percy reviews
  and rewrites in his own voice; keep it updated at each week boundary.
- docs/to_know.md created (16 Sep): per-session memory for interview prep,
  Weeks 0–3 so far. Percy's convention: update it at the END of EVERY
  session (see Mentor conventions).
- Bronze row count re-checked 2026-09-17: still exactly 1,604,977 (+ 2
  quarantine). CORRECTION to the earlier note: nothing was re-run after
  Day 2. The 1,604,979-message backlog was ALWAYS two producer sessions
  (8 Sep ~19:40-20:xx UTC, short; 9 Sep ~16:00-19:xx UTC, the bulk), so
  bronze has two date= folders but the same rows. Folder sizes:
  date=2026-09-08 ~4 MB (hours 19-20), date=2026-09-09 ~37 MB (hours 16-19).
  Mentor predicted one folder and was wrong; Percy's count was right.
- ~~Still commit by filename~~ FIXED 24 Sep: repo copies were already LF
  (`git ls-files --eol` showed i/lf on all 25 CRLF-on-disk files, so
  renormalise had nothing to do). `.gitattributes` with `* text=auto`
  committed; `git add .` is safe again.
- check_bronze.py has grown into a scratchpad of commented-out exploration
  blocks (id counts, hypothesis tests, lowest_tts). Fine for now; when
  silver starts, move the useful queries into a proper
  streaming/explore_bronze.py or delete them, and restore check_bronze.py
  to its two-count job.

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
- **requirements.txt FIXED 24 Sep**: UTF-8, 7 curated pins (confluent-kafka
  2.15.0, python-dotenv 1.2.3, requests 2.33.0, pyspark 4.2.0, dbt-core
  1.11.12, dbt-snowflake 1.11.6, pytest 9.1.1). Lesson: PowerShell `>`
  writes UTF-16; create files in the editor. (Seen again with `echo hello >`
  = 16 bytes.)
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

## Week 3 — DONE 2026-09-23 (Spark Structured Streaming, local)

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

### Day 2 Step 2 — DONE (2026-09-11): bronze to Parquet, drill passed

**What was built (2026-09-11), committed:**
- `good` gets two partition columns: `withColumn("date", to_date("ingested_at"))`
  and `withColumn("hour", hour("ingested_at"))`. Percy did this from a hint
  in one swing (pandas mapping `.dt.date` / `.dt.hour` given first).
  Schema verified: 22 fields, `date: date`, `hour: integer`.
- **DECISION (Percy's): bronze partitions from ingested_at**, not event_ts.
  Reason in his words: "easier to control; event_ts belongs in silver where
  the watermark lives." Framing for README: bronze answers "when did WE
  receive it", silver answers "when did it HAPPEN"; replaying Kafka lands in
  the same bronze folders regardless of TfL timestamp lateness.
- good sink: format parquet, path data/bronze/arrivals, checkpoint
  data/checkpoints/bronze_arrivals, partitionBy(date, hour), append, 10 s.
- bad sink: format text, path data/quarantine/arrivals, checkpoint
  data/checkpoints/bronze_quarantine, no partitionBy (text sink needs exactly
  one string column — `bad` is `.select("value_str")`, fine).
- `.gitignore` now has `data/` (checked before first run: it only had
  rabbitmq-data/ and activemq-data/).
- File sinks print nothing per batch. Heartbeat moved to the filesystem:
  `data/checkpoints/bronze_arrivals/commits/` gains one file per finished
  batch (named 0, 1, 2, ...). Percy used this as the "batch counter".
- **Kill/restart drill:** Ctrl+C when highest commit was **35**; restart
  resumed at **36** (not 0); drained to **80** = 81 batches
  (1,604,979 / 20,000 rounds up to 81 — prediction matched).
- `streaming/check_bronze.py` (committed): batch reader, local[2] + UTC, no
  Kafka package. Results: `spark.read.parquet("data/bronze/arrivals").count()`
  == **1,604,977** ✅; `spark.read.text("data/quarantine/arrivals").count()`
  == **2** ✅ and show() printed `this is not json` and `{"hello": "world"}`
  — this also closed the deferred proof from sub-step 3.
- No recurring "falling behind" WARN reported during the Parquet drain.

**Concept nailed down (oral check, took the full two swings + answer):**
Two ledgers, two jobs. Checkpoint = which Kafka offsets are done → prevents
GAPS and sets the restart point. `_spark_metadata/` inside the Parquet path
= catalogue of which part files each batch officially owns, written only
after all the batch's files are on disk → prevents DUPLICATES.
`spark.read.parquet` reads the catalogue, not the folder listing, so
half-written orphan files from the killed batch 36 are invisible to Spark.
Percy's first answer explained the 81-batch arithmetic (correct, wrong
question); second answer was "checkpointLocation?" (half: the gaps half).
Recorded in NOTES.md: "checkpoint prevents gaps, _spark_metadata prevents
duplicates, and only Spark reads the second one."

**CARRY-FORWARD for Week 7 (do not solve now):** only Spark honours
`_spark_metadata`. pandas, DuckDB, Snowflake COPY INTO and Snowpipe list the
folder and WOULD count orphan files as real rows. Bronze-to-S3-to-Snowpipe
needs either (a) no orphans landing (check Spark 4.x file-sink cleanup
options live when we get there) or (b) dedupe downstream anyway. This is
also the honest motivation for Iceberg/Delta: a catalogue every engine
reads. Interview-grade extension of the "kill at 35" story.

Optional check never run: `(Get-ChildItem -Recurse data\bronze\arrivals
-Filter *.parquet).Count` vs total paths listed in `_spark_metadata` entries;
disk ≥ catalogue, difference = orphans.

### Day 2 Step 2 history (2026-09-08/09): sub-steps 1-3

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

### Day 3 design — DONE (2026-09-15/16): both questions answered by measurement

All exploration was BATCH over bronze Parquet (`spark.read.parquet`) in
check_bronze.py, following the measure_skew → bronze pattern. Every step
had a prediction first. Percy iterated several of these himself.

**Q1 — what is "one arrival" / dedupe key. Findings, in order:**
- `groupBy("id").count()`: 16,822 distinct TfL ids; rows per id min 1 /
  p50 90 / p99 285 / max 1,760. So `id` IS stable across polls (Percy's
  prediction, correct) — a prediction lives ~45 min at 30 s polls.
- The 1,760-row id: 1 naptan (Uxbridge), 1 vehicle_id = **"000"**, 234
  distinct event_ts. Rows per poll ≈ 7–9. Inspection showed 3 different
  trains (time_to_station ~332/812/932 s) × 3 platforms (1, 2, 4), all
  with vehicle "000" = TfL's placeholder for "train not identified" (common
  at termini). TfL's `id` collapses all unidentified trains at a station
  into one id. `direction` was NULL on these rows.
- vehicle_id == "000" is **1.06 %** of bronze rows.
- Hypothesis test on non-000 rows, groupBy(line_id, vehicle_id, naptan_id,
  event_ts).count(): max 18, avg 1.50. Adding platform_name → max 6;
  adding current_location → max 4, avg 1.39. Remaining duplicates were
  IDENTICAL predictions fetched in two consecutive polls (same event_ts,
  same time_to_station, same expected_arrival; only ingested_at differs,
  ~58 s apart). I.e. TfL does not regenerate every 30 s; the producer
  re-fetches unchanged generations. This is the SAME mechanism as the
  8 Sep "identical event_ts across batches / steady 48 s skew" mystery —
  now understood, no longer "unexplained".
- Three named sources of row multiplication: (1) platform hedging (one
  train predicted on several platforms), (2) vehicle "000" collapse,
  (3) re-fetched unchanged generations.

**Q1 decisions (Percy's, in NOTES.md):**
- Three levels: bronze ROW (one photo of one board entry) → PREDICTION
  (one TfL generation for one train at one station; identity
  `(line_id, vehicle_id, naptan_id, event_ts)` = the
  dropDuplicatesWithinWatermark key) → ARRIVAL (one train approaching one
  station across many predictions; identity `(line_id, vehicle_id,
  naptan_id)`).
- `ingested_at` is OUT of the dedupe key on purpose (it differs on every
  row by construction; including it would make dedupe a no-op). Percy
  connected this to Week 2 dedupe.py's fingerprint excluding volatile
  fields.
- vehicle_id "000" rows are EXCLUDED from the delay metric and COUNTED as
  "unattributable" — that count is a data-quality metric for the dashboard.
- platform_name: mentor's lean = OUT of the key, keep one row arbitrarily
  (1 s difference is noise at 5-min grain). Agreed; recorded in README D13.
  (Percy asked what was expected of him here: nothing beyond writing the
  decision down — it is NOT in the key.)

**Q2 — what is an "actual" arrival. Findings:**
- Percy's first idea: arrived when time_to_station reaches 0. Tested:
  per arrival (non-000, groupBy arrival key, min(time_to_station)), then
  summary across 16,928 arrivals: min 1 / p50 **18 s** / avg 117 s /
  p99 **1,580 s** / max 1,829 s. So the countdown almost never reaches 0
  (30 s poll misses the last seconds) and ~1 % of arrivals vanish while
  still 25+ min out (cancelled/reversed/lost, OR truncated because the
  producer was stopped — the Day 1 "last window never finalises" problem
  in another form).
- Share of arrivals with lowest time_to_station <= 60 s: **86.6 %**
  (mentor predicted 85–92 %).

**Q2 decisions:**
- An arrival is COMPLETED when its lowest observed time_to_station <= 60 s
  (two polls). Reason recorded by Percy: p50 = 18 s. Above 60 s = not
  completed: cancelled / lost / truncated; counted, not measured.
- Delay definition — **STILL OPEN, ask first thing next session.** Two
  candidates were presented:
  (a) prediction drift = last expected_arrival − first expected_arrival
      (uses only TfL clocks → event-time only → reproducible on Kafka
      replay; mentor's lean, argued from Percy's own Day 1 sentence);
  (b) (last ingested_at + last time_to_station) − first expected_arrival
      (mixes in processing time → not reproducible on replay).
  DECIDED 16 Sep: (a), written in NOTES.md and README D14. Caveat in
  README: the first prediction (~45 min out) is itself an
  estimate, so "delay" = how much TfL's own forecast slipped, NOT lateness
  against a timetable (there is no timetable in this feed).
- Session-end truncation must be documented as a known limitation; it
  also feeds the Week 9 open design question about the last window.

**Spark/pandas mappings that landed this day (reuse them):**
- `groupBy("id").count()` ≈ `groupby("id").size().reset_index(name="count")`;
  Spark names the column `count` for you, and `min_("count")` then refers
  to that column (Percy asked why "count" worked without creating it).
- Two-step summary pattern: step 1 groupBy(key).agg(min(x).alias("m")) →
  one row per entity; step 2 `.agg(...)` on "m" → summary ACROSS entities
  (≈ `groupby(key)[x].min().describe()`). Percy collapsed both into one
  agg once; the two-step shape needed to be shown as code.
- `count("*")` (function) vs `"count"` (column name) in one agg is legal
  but confusing; rename with withColumnRenamed if it recurs.
- Spark recomputes a DataFrame each time an action runs on it (he called
  `.count().show()` and then reused the grouped object). Harmless at 1.6 M
  rows; mention caching only when it matters.

### Day 4 part 1 — DONE (2026-09-17/18): silver arrivals in BATCH, two design bugs fixed

File: `streaming/silver_arrivals.py` (batch, local[2], UTC, no Kafka
package; reads `data/bronze/arrivals`). Commit feb26b5. Every sub-step had
a prediction first; three mentor predictions were WRONG and are recorded.

**Concepts (Day 3 concepts, done first, oral answers OK after two swings):**
state = what Spark must remember to finish a job (seen keys, open sums);
state must be bounded or RAM overflows; the watermark is the ONE number that
both decides what is too late to accept AND what is safe to forget.
`dropDuplicatesWithinWatermark` chosen over plain `dropDuplicates` because
it bounds state regardless of whether event_ts is in the key (our key
happens to include it, so both would work; we chose the one whose safety
does not depend on that accident). Append mode = window written once when
finalised; the Week 9 "last window never finalises" question still parked.

**Sub-step results (all on bronze count 1,604,977):**
1. unattributable (vehicle_id == "000") = 17,086 (1.065 %); kept = 1,587,891;
   sum exact.
2. `to_timestamp(expected_arrival)`; consistency test
   `expected_arrival == event_ts + time_to_station` (cast long) held for
   1,587,891 / 1,587,891 = 100 %. So expected_arrival is DERIVED by TfL;
   only two of the three time fields carry information. Free dbt test for
   Week 5 (expect 100 %).
3. `dropDuplicates([line_id, vehicle_id, naptan_id, event_ts])` ->
   1,056,017 predictions. Ratio 1.504, matches Day 3's measured 1.50 for
   this key to three decimals.
4. First arrivals table with groupBy(line_id, vehicle_id, naptan_id):
   16,928 rows (= Day 3), n_predictions p50 61 / max 180. Completed share
   86.37 % vs Day 3's 86.60 % — explained and VERIFIED (86.60 % re-measured
   before dedupe): dropDuplicates keeps an ARBITRARY platform-hedging row,
   siblings carry different time_to_station, ~40 arrivals lost their <=60 s
   row. Accepted and documented; min/max are duplicate-proof anyway, only
   n_predictions and state size need the dedupe.
5. Delay: FIRST RESULT WAS NONSENSE — max_delay_s = 86,736 s (24.1 h),
   p50 6,624 s, 9,633 of ~14.6k completed arrivals "delayed" > 1 h, 3,975
   > 80,000 s. Mentor predicted "a few dozen" cross-day cases; wrong by two
   orders of magnitude. ROOT CAUSE: the arrival key (line, vehicle, naptan)
   has NO TIME BOUND. Bronze spans two sessions ~24 h apart, and within one
   3-4 h session a tube train visits the same station 2-3 times (round trip
   1-2 h). Day 3's 16,928 was never "arrivals"; it was distinct
   (line, train, station) combinations.

**Bug fix 1 — session_window (Percy's one-line change, worked first run):**
`groupBy(session_window("event_ts", "10 minutes"), line_id, vehicle_id,
naptan_id)`. A new session starts after >10 min with no prediction for that
key. Input rows unchanged (1,056,017); output = one row per TRIP.
Result: 39,222 arrivals (mentor predicted 35-50k). max_delay 86,736 -> 9,940 s;
p50 6,624 -> 155 s; "> 1 h" count 9,633 -> 43.
Gap sensitivity (mentor predicted "within a few percent" — WRONG, ~5.5 %
per step): 5 min = 41,450 / 10 min = 39,222 / 15 min = 37,119. Means real
5-15 min gaps exist inside trips (TfL loses a train briefly) and/or repeat
visits closer than 15 min (Waterloo & City ~10 min shuttle). DECISION:
10 minutes, recorded as a judgement WITH the sensitivity table; revisit
after a week of data by measuring the per-key gap distribution directly
(needs a lag window function — not taught yet).
Percy hit a wall on session_window ("does it multiply rows?"); what
unblocked him: one concrete train timeline (16:05-16:48, 70 min silence,
17:58-18:41) and the sentence "input rows unchanged, more PILES because
there really are more trips".

**Bug fix 2 — delay definition:** `max(expected) - min(expected)` is the
SPREAD of forecasts and can never be negative (min_delay_s was 0). Day 3's
definition meant first/last IN TIME by event_ts. Fixed with
`min_by("expected_arrival_ts","event_ts")` / `max_by(...)` (verified in
pyspark.sql.functions on 4.2.0 by running it).
Completed-only (10-min gap) delay: n 28,854 | min -1,371 s | avg 147 s |
p50 41 s | p99 1,932 s | max 9,939 s; 41 completed arrivals > 3,600 s
(terminus dwell / long-gap edge cases, footnote not flaw).
**Completed share is now 73.6 %** (28,854 / 39,222), down from 86.4 %:
splitting merged trips creates fragments whose lowest tts never reaches
60 s. Honest, documented, another reason to measure the gap distribution.

**Interview sentences produced today (Percy to rewrite in his words):**
- "My arrival key was correct in space but had no bound in time; I found it
  because a 24-hour 'delay' is impossible, fixed it with a session window,
  and the before/after is 9,633 -> 43 impossible delays."
- "dropDuplicates keeps an arbitrary row; I measured what that cost
  (86.6 -> 86.4 %) and documented it rather than pretending it was free."
- "expected_arrival is derived (100 % consistent with event_ts +
  time_to_station), so it is a free data-quality test, not a fact."
- "The watermark does two jobs with one number: too late to accept, and
  therefore safe to forget."

**Code state (`silver_arrivals.py`, flat script, not yet functions):**
imports include session_window, min_by, max_by; `.cache()` on grouped_kept
(used 3+ times — first place caching earned its keep, taught as "Spark is
lazy, a DataFrame is a recipe until cached"); consistency check kept as a
comment for the pytest step. Known cosmetics: print label says "with delay"
(should say "> 3600 s"); delay summary must filter completed first.

**Teaching notes for next mentor (17-18 Sep):**
- Percy answers a NEIGHBOURING question confidently (60 s completion rule
  when asked about state eviction). Re-ask pointing at the failure mode.
- He skipped a "say the streaming sentence out loud" request twice. Ask
  once, then just state it and move on; don't nag.
- One-line code changes with "run it and check ONE number" worked well
  when he was overwhelmed. "What are we even doing?" sentence at the top.
- Three mentor predictions were wrong today (one-folder, few-dozen cross-day,
  gap sensitivity). Saying so plainly kept trust; keep doing it.
- He asked "check the code" three times; he wants review, not just numbers.
  Read the file each time (device_bash on the mounted folder works).

### Day 4 part 2 — DONE (2026-09-21/22): windows, functions, pytest

**Step 6, tumbling windows (window_reliability):**
`groupBy(window("last_event_seen", "5 minutes"), line_id, naptan_id)` over
ALL arrivals (not just completed), with conditional aggregates:
`count(when(col("completed"), 1))` = n_completed,
`avg(when(col("completed"), col("delay_s")))` etc. for the delay stats,
`count(when(~col("completed"), 1))` = n_incomplete. Percy's column names:
n_total / n_completed / n_incomplete / avg,min,max,p50,p99_delay_s.
Results on bronze 1,604,977: completed-only first pass = 12,330 windows,
sum(n_arrivals) == 28,854 exactly. With not-completed included = **13,602
windows**, sum n_completed 28,854 / n_incomplete 10,368 / n_total 39,222.
Mentor predictions: rows 15-25k WRONG (12,330 — ~2.3 completed arrivals
per line/station/5-min bucket, denser than guessed); 13,000-14,500 for the
second pass RIGHT. Windows with 0 completed show NULL delay stats: correct,
do NOT coalesce to 0 (0 s delay and "no data" are different facts).
Observed: first window of a session (19:40-19:45 on 8 Sep) has many
negative delays because trains were mid-approach when collection started.
NOTES.md line written: first and last windows of every collection session
are edge-truncated; exclude or flag them in the marts (Week 5).
Concept taught and landed (Percy's words in NOTES.md): session_window
splits by SILENCE in the data and answers "which trip"; window splits by
the CLOCK and answers "which reporting bucket". Both are piles, different
rules.

**Step 7, refactor:** silver_arrivals.py now = six pure functions
(DataFrame in, DataFrame out, no reads/shows/prints inside):
`split_unattributable(df) -> (kept, unattributable)`, `add_expected_ts`,
`dedupe_predictions`, `build_arrivals(df, gap="10 minutes")`, `add_delay`,
`window_reliability(df, window_duration="5 minutes")`. SparkSession, the
bronze read, `.cache()` on grouped_kept and the two shows live under
`if __name__ == "__main__":`. Percy did all six in one swing; review fixed:
missing __main__ guard (would make `import` read bronze), useless
`.cache()` on a DataFrame used once, dead commented code, unused import.
Numbers identical before/after (28,854 / 10,368 / 39,222 / 13,602).

**Step 8, pytest:** `streaming/__init__.py` (empty, makes it a package);
`tests/conftest.py` with a session-scoped `spark` fixture (local[1], UTC,
`spark.sql.shuffle.partitions=1` — default 200 makes 5-row groupBys slow);
`tests/test_silver_arrivals.py` with 5 tests, all green (`5 passed`, ~20 s,
all Spark startup). Run with `python -m pytest tests -q` from repo root
(`python -m` puts cwd on sys.path so `from streaming.silver_arrivals
import ...` resolves). Tests: (1) "000" split incl. smoke_test row dropped
from kept but NOT counted unattributable; (2) dedupe collapses platform
siblings, new event_ts survives, asserted on sorted collected timestamps;
(3) two trips 70 min apart -> 2 arrivals (guards session_window);
(4) delay = -120 when forecast improves 16:30 -> 16:28 (guards
min_by/max_by; plain min/max would give +120); (5) completed boundary
lowest_tts 60 -> True, 61 -> False. Percy's oral answer on "16:14 instead
of 17:58" was correct and clearly worded (within 10 min of 16:08 = same
pile). PySpark round-trips datetime via the machine's local zone (BST);
symmetric, so datetime-to-datetime comparisons are safe, string
comparisons are not. pytest is installed in .venv but NOT yet in
requirements.txt.

Cosmetic carry-over (not blocking): rename `test_build_arrivals` ->
`..._splits_trips_70_min_apart` and `test_completed_rule` ->
`..._at_60_and_61`; typo `sigblings`; strip ~30 trailing blank lines in the
test file; a mentor `git status` left a stale `.git/index.lock` once
(Percy removed it) — mentor should avoid running git in the mounted repo.

**Teaching notes for next mentor (21-22 Sep) — READ THESE:**
- Percy pushed back twice this session, both times rightly: (a) "you talk
  in shorthand, short messages jumbled together" after I packed three ideas
  + six function names into one message; (b) "your hints can be vague or
  complex". What fixed it: ONE idea per message, a "what are we even
  doing" sentence at the top, then ONE tiny action with a predicted number.
  For hints: give the CONCRETE rows/values as a small table and the exact
  expected result, let him write the code. Do not describe the shape of a
  solution in prose and call it a hint.
- He misread "refactor into functions" as "create a streaming file". Say
  explicitly what a step is NOT when the name could mislead.
- When he has a pattern (he had transform.py from Week 2) he will do all
  six functions in one go unprompted. Let him, then review.
- He asks "check the code" after each step and means it. Read the file.
  Review format that worked: numbered, most important first, each item =
  what + why + the fix, then one paragraph of what is right.
- Predicted outputs before every run still works; he checks them and
  notices when mine are wrong. Two mentor misses this session (12,330 rows;
  earlier 15-25k guess). Say so plainly.

### Step 9 + Day 5 — DONE (2026-09-23): streaming switch, Parquet sink, gap measured, drill passed

Commit 5f79d0a. Files: `streaming/silver_arrivals.py` (six functions unchanged;
new `dedupe_predictions_within_stream`; `__main__` is now a streaming job),
`streaming/check_silver.py` (batch reader over silver + bronze max event_ts).

**Verified live (spark.apache.org, 4.2.0 docs, 2026-09-23):** file-based
streaming sources require the schema (or `spark.sql.streaming.schemaInference`);
chaining multiple stateful operators is supported in Append mode (banned in
Update/Complete); session_window does not support Update mode and needs at
least one extra grouping column. Docs do NOT say whether session_window may
follow dropDuplicatesWithinWatermark — we RAN it and it works on 4.2.0.

**Sub-steps and results (prediction -> actual):**
- 9a schema: `bronze_schema = spark.read.parquet(...).schema` then
  `spark.readStream.schema(bronze_schema).option("maxFilesPerTrigger", 200)
  .parquet(...)`. isStreaming True, 22 fields. Prediction matched.
- 9b watermark: `dedupe_predictions_within_stream(df)` =
  `withWatermark("event_ts","2 minutes").dropDuplicatesWithinWatermark([key])`.
  Batch `dedupe_predictions` kept for the tests. 23 fields after
  add_expected_ts. Prediction matched. Percy wrote it from a hint in one swing.
- 9c console sink: dedupe -> session_window -> add_delay in ONE Append query.
  NO error (two stateful ops chained). Batches 0-3 EMPTY, Batch 4 first rows
  (19:40 sessions). Mentor predicted rows by Batch 1: WRONG. Reason: Append
  emits a session only when watermark > session end (= last event + 10 min
  gap); watermark advances only as new events arrive; watermark from batch N
  applies in batch N+1. Each 10-file batch moved the clock ~2 min, so four
  batches to pass 19:50. `delay_s = -23` row proved min_by/max_by survived.
  Every batch 105-123 s vs 10 s trigger — RECURRING "falling behind" (two
  state stores, Windows, local[2]). Noted, not blocking.
- **D15 DECISION (Percy's): two jobs.** Job 1 = this stream -> Parquet
  `data/silver/arrivals`, checkpoint `data/checkpoints/silver_arrivals`.
  Job 2 = `window_reliability` as BATCH over silver (not yet written).
  His reason: own table per stage, debuggable when something breaks.
  Mentor's honest counter-argument for one job: purer streaming, one
  checkpoint, one failure surface, lower latency for the windows.
- 9d Parquet sink: first run with maxFilesPerTrigger=10 — mentor said "~50
  bronze files, 5-7 batches". WRONG: bronze has **774** Parquet files (81
  bronze batches x several date/hour partitions), so 78 batches x 2 min.
  Stopped at commit 10, changed to 200, deleted silver + checkpoint, reran:
  commits 0-4 (mentor predicted 4; the 5th batch read no files, it flushed
  sessions closed by batch 3's watermark). Silver = **801** Parquet files for
  34k rows: SMALL-FILES PROBLEM, parked for Week 7.
- 9e count: silver **34,179** rows / 27,031 completed / max last_event_seen
  **2026-09-09 19:03:56**. Batch 39,222 / 28,854. Gap **5,043 (12.9 %)**:
  1,823 completed + 3,220 incomplete. Mentor predicted "a few hundred to
  2,000": WRONG by 2.5x. Mentor's picture "last session per key" was also
  wrong. Right picture: a TIME CUT-OFF. Session written only when watermark
  (newest event − 2 min) > session end (last event + 10 min) => any session
  with last event later than newest − 12 min is stuck in state. VERIFIED:
  bronze max event_ts = **19:16:06**, silver max = 19:03:56, difference
  12 min 10 s. Rows are NOT lost: they sit in the checkpoint state store and
  would flush if one more event arrived. **D16** = this paragraph.
  Week 9 options (not implemented): document as expected (continuous
  collection never ends); graceful drain via synthetic future event; Update
  mode is OFF the table (session_window unsupported, docs).
- Day 5 drill: clean start, Ctrl+C ~1 min after commit 1, restart ->
  first new commit **2** (not 0); drained to 4; check_silver identical to
  the row (34,179 / 27,031 / 19:03:56). Disk parquet count 801 == clean run:
  mentor predicted orphans, got NONE (kill probably landed before the write
  stage of batch 2). Disk-vs-catalogue check has now been run once on silver.
- pytest still `5 passed` after the switch (functions untouched).

**Week 3 oral exam (no notes, 6 Qs asked at once at Percy's request):**
3 pass (schema, gap arithmetic, four-empty-batches), 2 half (watermark: gave
mechanism, missed bounded-state WHY; D15: gave own reason, missed the
counter-argument), 1 fail (two ledgers — knew "duplicates", couldn't name
checkpoint vs _spark_metadata under pressure; he HAS this in NOTES.md from
11 Sep). Pattern: reaches for mechanism, skips the failure it prevents.
Percy pasted the corrected answers into NOTES.md the same night.
**Re-drill Q2 and Q5 at the start of next session.**

**Teaching notes (23 Sep):**
- Four mentor predictions wrong today (rows by batch 1; ~50 files; gap size;
  orphans). Saying so plainly kept trust. COUNT things before predicting them
  (774 files was one `find | wc -l` away).
- "Check the code" x2, both times he wanted review; numbered list, most
  important first, worked again.
- He asked for all six exam questions at once and answered all six at once.
  Fine for him; mark most-important-first.
- One idea per message + predicted output held all session; no pushback on
  density this time.

**Code cosmetics still open:** dead commented `window_reliability` lines at
the bottom of `silver_arrivals.py` `__main__`; unused imports in both
silver_arrivals.py (`count_distinct`) and check_silver.py; check_silver
appName says "silver_arrivals".

### Week 4 — STARTED 2026-09-24 (S3 + Snowpipe)

**Week 3 close-out done 24 Sep** (all committed by Percy): exam re-drill Q2
pass, Q5 pass on outcomes (tidy: "resumption point" = gaps side; duplicates
come from half-written files; only Spark reads _spark_metadata). README
D15 (two jobs + counter-argument), D16 (12-min cut-off, numbers), D17
(bronze to S3/Snowflake, thick-Spark paragraph), Numbers table extended
(both completed shares 86.6 % / 73.6 % labelled by stage). Job 2:
`streaming/window_reliability_batch.py` (`__main__` guard, appName
window_reliability_batch, imports `window_reliability` from
silver_arrivals, writes `data/silver/window_reliability` mode overwrite):
12,546 windows, sums 27,031 / 34,179 match silver exactly, 2 Parquet files
(vs silver's 801). Mentor predicted 11,500-12,500: just outside. Cosmetics
done (dead code, unused imports, check_silver appName). Noted for Week 5:
`dbt/.user.yml` is tracked, should be gitignored.

**Design decisions 24 Sep:**
- D17: bronze arrivals -> S3 raw -> Snowpipe -> RAW. Silver tables also to
  S3 (processed prefix) and read by dbt in Week 5. Percy asked "what is the
  point of Spark if dbt cleans it": answered as thick-Spark vs thin-Spark,
  both used together everywhere, line drawn by latency need; overlap =
  bronze cast twice (Spark + dbt staging), accepted. Week 10 idea: rebuild
  silver in dbt SQL, check engines agree.
- Week ordering: S3 buckets (Day 1) and Spark->s3a (Day 2) BEFORE creating
  the Snowflake trial (Day 3), so the 30-day clock does not start early.

**Day 1 DONE 24 Sep:** buckets `tfl-reliability-raw-percy` and
`tfl-reliability-processed-percy`, eu-west-2, ACLs disabled, block public
access on, versioning off, SSE-S3. Created in console as IAM user
`percy-tfl-user` (Percy was first logged in as root; sent back; root only
for billing/emergencies; CloudShell not permitted for the IAM user, not
needed — local PowerShell `aws` CLI is what we use). Write test: `aws s3 cp`
/ `ls` / `rm` on `_test/hello.txt` all OK, so PutObject/DeleteObject are
granted. Percy writes the click-by-click in NOTES.md.

**Verified live 24 Sep (hadoop.apache.org r3.5.0 docs, not memory):**
- Bundled Hadoop in PySpark 4.2.0 is **3.5.0** (checked on Percy's machine
  via `VersionInfo.getVersion()`). So the connector is
  `org.apache.hadoop:hadoop-aws:3.5.0`, which pulls the AWS SDK **v2**
  `bundle` jar transitively via spark.jars.packages (big download, ~500 MB
  class of jar; first run slow, then cached in .ivy2.5.2).
- Default `fs.s3a.aws.credentials.provider` chain = TemporaryAWS, SimpleAWS,
  EnvironmentVariable, IAMInstance. It does NOT read ~/.aws/credentials.
  To reuse the CLI's profile file set
  `spark.hadoop.fs.s3a.aws.credentials.provider =
  software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider`
  (documented in the 3.5.0 authentication page). Alternative: env vars
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (already in default chain).

### NEXT (start here): Week 4 Day 2 — Spark bronze sink to s3a://

Pre-flight: fresh terminal, venv active, `python -m pytest tests -q` -> 5
passed; `aws s3 ls` -> two buckets; Docker Kafka up only if streaming live.

Teaching plan (one sub-step per message, prediction before each run,
hint-level since he has bronze_arrivals.py and check_bronze.py):
1. **Smoke test, batch, tiny**: new `streaming/s3_smoke.py` (or inline):
   SparkSession with `spark.jars.packages=org.apache.hadoop:hadoop-aws:3.5.0`
   + the ProfileCredentialsProvider config + UTC; `spark.range(5).write
   .mode("overwrite").parquet("s3a://tfl-reliability-raw-percy/_test/range")`
   then read back `.count()` == 5. Predicted: first run downloads many jars
   (minutes); then `aws s3 ls s3://tfl-reliability-raw-percy/_test/range/`
   shows part files + _SUCCESS. Possible Windows gotchas: hadoop.dll already
   fixed; s3a needs no winutils beyond what is in place. If
   `NoSuchMethodError`/`ClassNotFound` -> SDK bundle version mismatch, check
   ivy cache pulled `software.amazon.awssdk:bundle`.
2. **Copy existing bronze to S3 once (batch)**: `spark.read.parquet
   ("data/bronze/arrivals").write.partitionBy("date","hour").mode
   ("overwrite").parquet("s3a://tfl-reliability-raw-percy/arrivals")`.
   Predicted: read back count == 1,604,977; `aws s3 ls --recursive |
   Measure-Object -Line` ~ hundreds of objects (774 local files, but a
   batch write repartitions; count before predicting). Cost: ~40 MB, pence.
   Design point: this is a backfill; the STREAM (step 3) is the real path.
3. **Streaming sink**: bronze_arrivals.py `good` sink path ->
   `s3a://.../arrivals`, checkpoint stays LOCAL for now (`data/checkpoints/
   bronze_arrivals_s3`, fresh dir so it replays from earliest). Discuss:
   checkpoint on S3 is possible but S3A rename semantics; local is fine for
   Week 4, revisit Week 7. Trigger: plan says ~5-min micro-batches for S3
   (small-files trade-off); use `processingTime="5 minutes"` for the S3
   sink and record the decision + the compaction TODO (plan step 2: hourly
   compaction job to 64-256 MB files). Predicted: same count as local after
   drain. This overwrites step 2's backfill: decide whether step 2 is even
   needed (mentor lean: skip step 2 if Kafka still has the 8-9 Sep backlog
   — check `kafka-get-offsets`; retention 30 d from 8 Sep = until 8 Oct).
4. `_spark_metadata` on S3 + non-Spark readers (Snowpipe) = the Week 7
   carry-forward becomes live NOW: Snowpipe will list the folder. Decide
   whether to point Snowpipe at the stream's folder (orphan risk, measured
   as zero so far) or at a compacted folder. Record as D18.
5. README: bucket names, region, IAM-not-root, hadoop-aws 3.5.0 +
   ProfileCredentialsProvider decision, 5-min trigger decision.
Then Day 3: `snowflake/bootstrap.sql` WRITTEN FIRST (rebuild from
DW_setup.sql + Week 1 to_know notes: warehouse, DB, RAW/STAGING schemas,
file format PARQUET, storage integration, external stage, RAW.ARRIVALS
table, Snowpipe AUTO_INGEST), then new trial (Standard/AWS/eu-west-2), run
it, storage-integration IAM handshake (plan: budget half a day, external ID
trips everyone), S3 event notification -> SQS. Day 4: latency measurement +
reconciliation script (Kafka offsets vs RAW count per hour). Day 5:
done-when + oral exam.

Carry-over still open: bronze for tfl.line-status and tfl.disruptions
(simple copies, no watermark); retention.ms on those two topics
unconfirmed; check_bronze.py tidy; small-files problem (Week 7); orphan
files vs non-Spark readers (Week 7); per-key gap distribution to revisit
the 10-min session gap (needs lag window function, not taught yet).

### Reference — original Day 3/4 sub-step plan as written 16 Sep (steps 1-5 done 17-18 Sep)

Start here next session. Pre-flight: docker ps (kafka up — optional for
batch work), venv active, fresh terminal (PATH has C:\hadoop\bin). First
question to Percy: confirm he has reviewed README.md and docs/to_know.md
and corrected anything not in his words.

Then Day 3 concepts (postcards analogy again, tie to his own data):
- Stateful streaming: why dedupe and windows need STATE, why state must
  be bounded, and how the watermark is the thing that bounds it (this is
  the "WHY before mechanism" lesson from the Day 1 oral exam).
- `withWatermark("event_ts", "2 minutes")` + `dropDuplicatesWithinWatermark`
  on the chosen key (documented in 4.2.0; verified 2026-09-04).
- 5-minute tumbling windows per (line_id, station) using `window()`;
  Append output mode → a window is emitted once when finalised.
- Silver reads bronze Parquet (not Kafka): `spark.readStream.parquet(...)`
  on a partitioned path needs the schema supplied or
  `spark.sql.streaming.schemaInference` — verify live; batch-first
  development sidesteps this until the switch.
- Silver is where casts happen: expected_arrival string → timestamp; free
  consistency test expected_arrival == event_ts + time_to_station.

Then code, sub-steps with predictions like Day 2. pytest on static
DataFrames for the transform functions (plan requires it). Proposed
sub-steps for streaming/silver_arrivals.py, batch first (mentor's draft —
adjust with Percy; one sub-step per message):

1. **Batch read + casts.** spark.read.parquet(bronze) → filter event_type
   == "arrival" and vehicle_id != "000" (count the excluded rows and print
   them) → cast expected_arrival string → timestamp. Predicted: row count
   = current bronze total minus the 000 rows (he has the 1.06 % figure).
   Free consistency test: expected_arrival == event_ts + time_to_station
   (to the second) for ~100 % of rows.
2. **Dedupe to predictions (batch version).** dropDuplicates on
   (line_id, vehicle_id, naptan_id, event_ts). Predicted: rows shrink by
   roughly the 1.39 avg factor measured on Day 3 (i.e. to ~72 % of input).
   Say out loud: in streaming this becomes withWatermark("event_ts",
   "2 minutes") + dropDuplicatesWithinWatermark(same key), and the
   watermark is what lets Spark forget old keys (bounded state).
3. **Arrivals table.** groupBy(line_id, vehicle_id, naptan_id).agg(
   min(expected_arrival) as first_expected, max(expected_arrival) as
   last_expected, min(time_to_station) as lowest_tts, max(event_ts) as
   last_seen_ts, count as n_predictions). completed = lowest_tts <= 60.
   Predicted: ~16.9 k rows, ~86.6 % completed (Day 3 numbers).
4. **Delay per completed arrival** per the chosen definition; sanity
   summary (min/p50/p99/max of delay_s). Predicted: p50 near 0, some
   negative (train arrived earlier than first predicted), tail in minutes.
5. **5-minute tumbling windows** per (line_id, naptan_id) on last_seen_ts
   (the arrival's event time): avg/p50/max delay, n_arrivals, n_not_completed.
   Batch window() first; then switch read → readStream with
   withWatermark + Append output mode and re-check the same numbers.
6. **pytest on static DataFrames**: dedupe key, completed rule, delay
   calc, consistency test. Feeds Week 8 CI.
7. Day 5: checkpoint + kill/restart drill on the silver stream.

Note for the streaming switch: groupBy(arrival key) over an unbounded
stream is itself stateful; the arrival table in streaming form needs a
watermark on event_ts too, and an arrival is only "final" once the
watermark passes its last_seen_ts — that is exactly where the Week 9
"last window never finalises" question lives. Teach it when we get there,
not before.

Teaching notes from Day 3 (2026-09-15/16):
- He said explicitly: "I understand things slowly and it might be hard if
  complex things are thrown/described all at once." When I packed the Q2
  reframe + two delay definitions + a measurement into one message he came
  back with "I am not sure I understand the next steps". Backing up to ONE
  sentence of goal, ONE sentence of problem, ONE measurement fixed it.
  Keep to one idea per message, and put the "what are we even doing"
  sentence at the top.
- Measurement-driven design works very well for him: prediction → run →
  read the table together → decide. He iterated the hypothesis test three
  times unprompted. Let him drive when he has momentum.
- Numeric leading questions land ("1,760 rows but only ~460 polls: what
  does that force?"). Use arithmetic as the hint.
- When a mentor guess is wrong (I guessed line_id would vary; it didn't),
  say so plainly — he notices and it builds trust in the prediction habit.

Teaching notes carried from Day 2 (2026-09-11), still true:
- Hint-level worked for withColumn/to_date/hour and for mirroring the
  quarantine sink from the good sink. Full code with line comments was
  right for the first Parquet writeStream (new surface).
- Oral checks: he tends to answer a neighbouring question confidently and
  well (81-batch arithmetic when asked about duplicates). Re-ask pointing
  at the failure mode, then give the answer after two swings; that landed.
- Pre-flight + folder-watching in a second terminal worked well; keep the
  "prediction before every run" rhythm.

Reference — Day 2 sub-step plan as originally written (all now done):

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
  skew measurement table + 2-minute watermark decision with caveats;
  bronze-partitioned-by-ingested_at decision; checkpoint vs
  _spark_metadata explanation + kill-at-35 drill result.
- Week 7: orphan Parquet files vs non-Spark readers (see Day 2 DONE).
- TODO(you): confirm retention.ms=30d was applied to tfl.line-status and
  tfl.disruptions, not just tfl.arrivals (still unconfirmed on 2026-09-11).
- ~~UNEXPLAINED (from 2026-09-08): steady ~48 s skew on one central-line
  partition.~~ EXPLAINED on Day 3: unchanged TfL generations re-fetched
  across consecutive polls (same event_ts, later ingested_at). Still worth
  a per-line skew number in silver, but it is no longer a mystery.
- check_bronze.py: tidy the exploration blocks (see Project status).
- README: written 16 Sep as a draft (D1–D14). Percy to review; fill TODOs
  at each week boundary; add the "why local compute" cost numbers in Week 7.
- Git line-ending renormalisation (see Environment) — not done on
  2026-09-11 either; still commit by filename.
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
  After each session update a different note (to_know.md, create if not created, from the first day to now, try and remember): containing key things done to remember what each session is about for future reading and interview prep
