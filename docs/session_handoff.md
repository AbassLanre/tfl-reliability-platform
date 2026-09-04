# Session Handoff — TfL Reliability Platform

Paste/attach this into a new Claude session along with the build plan PDF
(tfl_reliability_platform_build_plan_v1_2.pdf). Invoke the
data-engineering-mentor skill first.

## Who I am / how to work with me

- PercyAbs. Comfortable-ish: Python, SQL, Postgres, dbt, Airflow, Docker, Git.
  Newer to: Snowflake, Kafka (learned Week 2). Spark is BRAND NEW as of
  Week 3 — first time ever with the framework. Plain English, analogies,
  one idea per message.
- Agreed work style: "mentor guides, Percy types/runs everything himself."
  One small step at a time; verify current facts against live docs and say
  what's verified vs. from memory. Do NOT pack multiple actions into one
  dense instruction — unpack them.
- Hints before answers on learning exercises; escalate to partial code with
  TODO(you) gaps when stuck. Two swings, then the answer with reasoning.
- Every verification has a PREDICTED output; check the prediction, not just
  that it ran (lesson learned the hard way — see deduper bug below).
- Machine: Windows, PowerShell. Project folder:
  C:\Users\user\Documents\tfl-reliability-platform (connect it to the session).
- Keep NOTES.md entries for every bug/decision (Week 10 needs it). Percy
  keeps NOTES.md live DURING sessions, in his own words — this is working.

## Project status (as of 2026-09-04)

- Week 0 ✅  Week 1 ✅  Week 2 ✅  Week 3: Day 1 (concepts) ✅, Day 2 next

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
  ingested_at) — this is the Week 3 watermark material. Two junk smoke-test
  messages exist in tfl.arrivals (event_type="smoke_test") — deliberate,
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
- **winutils.exe / HADOOP_HOME NOT set.** Spark logs
  `WARN Shell: Did not find winutils.exe ... HADOOP_HOME and hadoop.home.dir
  are unset` on startup. Harmless for in-memory work; EXPECTED to break the
  first time Spark writes Parquet or a checkpoint to local disk (Day 2).
  Decision: fix it when it bites, with the real error in NOTES.md.
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

### Day 2 — NEXT. First step, exactly:

**Step 1: measure the skew** (first real Spark program; small; reads real
data; answers the watermark question). Batch read (spark.read, not
readStream) of tfl.arrivals from Kafka with startingOffsets=earliest,
CAST(value AS STRING), parse JSON against docs/schema.md (event_ts and
ingested_at are ISO8601 strings), filter event_type == "arrival" (drops
the two smoke_test messages), compute skew_s = ingested_at - event_ts in
seconds, then min/avg/p50/p99/max and a count. This needs the Kafka
connector for the first time: set
`spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`
on the SparkSession builder — first run downloads JARs via Ivy (slow,
network, prints a lot; that's normal). Kafka must be up
(`docker compose up -d`). Prediction to make BEFORE running: p50 near
43s, p99 unknown — that's the point. This is in-memory only, so winutils
should NOT bite yet. Put the script under streaming/ (e.g.
streaming/measure_skew.py) — it becomes README evidence.

Then per the plan: Day 2 bronze job (readStream, parse against schema,
dead-letter unparseable to a quarantine path, raw Parquet partitioned by
date/hour — THIS is where winutils will bite; fix = HADOOP_HOME +
winutils.exe + hadoop.dll for the matching Hadoop version, record in
NOTES.md). Days 3-4 silver (dropDuplicatesWithinWatermark on composite
key; expected vs actual arrival delay per line+station in 5-min tumbling
windows). Day 5 checkpoint + kill/restart drill. pytest on static
DataFrames throughout.

### Carry-over tasks (not blocking)

- README: no-dedupe-on-arrivals defence; no-Avro/Schema-Registry defence.
- requirements.txt: fix UTF-16 encoding, curate, add pyspark==4.2.0.
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
