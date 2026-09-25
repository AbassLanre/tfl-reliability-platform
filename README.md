# TfL Real-Time Service Reliability Platform

> **Status: work in progress (Week 3 of 10).** Local streaming stack
> (Kafka → Spark Structured Streaming → Parquet) is running; cloud landing
> (S3 + Snowpipe + Snowflake), dbt marts, Airflow, dashboard and Terraform
> follow in Weeks 4–9. Sections marked *TODO* are filled in as each week
> passes its "done when" checkpoint. The full decision log and post-mortem
> land in Week 10.

## The problem

**Which London Underground lines are actually reliable, by line, by station,
by time of day, and can we detect a disruption forming before TfL officially
declares it?**

TfL publishes performance data only in aggregate and in arrears. This
platform answers the question from live data: the TfL Unified API is polled
every 30 s, every prediction is kept, and reliability is computed from how
TfL's own arrival forecasts drift while a train approaches a station.

Tools are the *how*. This question is the *why*, and every decision below
traces back to it.

## Architecture

```
TfL Unified API
  | Python producer, polls every 30 s (arrivals) / 60 s (status, disruptions)   [ LOCAL ]
  v
Apache Kafka 4.3 (KRaft, single broker, Docker)                                 [ LOCAL ]
  topics: tfl.arrivals (11 partitions, keyed by line_id) | tfl.line-status (1) | tfl.disruptions (1)
  |
  v
Spark Structured Streaming 4.2 (PySpark, local)                                 [ LOCAL ]
  bronze: parse against docs/schema.md -> quarantine unparseable -> Parquet partitioned by date/hour
  silver: dedupe (watermarked) -> arrivals -> delay -> 5-min tumbling windows     <- in progress
  |
  v
Amazon S3 raw + processed Parquet, Snowpipe auto-ingest                          [ CLOUD, Terraform ]  TODO Week 4/7
  v
Snowflake RAW -> staging -> marts (dbt)                                         [ CLOUD ]            TODO Week 4/5
  +-- Airflow: daily aggregations, dbt runs, data quality                        [ LOCAL ]            TODO Week 6
  +-- Streamlit: live line reliability scores                                    [ LOCAL ]            TODO Week 6
```

Compute is deliberately local; only storage and ingestion go to the cloud.
Target total cloud spend for the whole project: under £5. *(Defended in
full in Week 7.)*

## Repository layout

| path | what |
|---|---|
| `ingestion/producer/` | TfL poller → Kafka. `config.py`, `tfl_client.py`, `transform.py`, `dedupe.py`, `main.py` |
| `streaming/` | Spark jobs. `measure_skew.py` (watermark measurement), `bronze_arrivals.py` (streaming bronze), `check_bronze.py` (counts + exploration) |
| `docs/schema.md` | Kafka message contract: envelope + per-topic payload, types, required/optional, TfL source field |
| `docs/sample_payloads/` | Saved TfL responses the transforms are tested against |
| `docker-compose.yml` | Kafka broker |
| `dbt/`, `airflow/`, `terraform/`, `dashboard/`, `snowflake/` | placeholders until their week |
| `NOTES.md` | Running log of every bug and decision, kept live during sessions |
| `docs/session_handoff.md`, `docs/to_know.md` | Mentoring handoff and per-session interview-prep memory |

## Message contract (docs/schema.md v1)

Every message carries an envelope: `schema_version` (int, starts at 1),
`event_type`, `ingested_at` (producer clock, UTC) and `event_ts` (the
source's own timestamp). Payload fields are snake_case, mapped from TfL's
camelCase at the producer. Required fields are read with `raw["x"]` so a
missing one fails loudly; optional fields with `raw.get("x")`.

Event-time quality differs per topic and that is documented rather than
hidden: arrivals have a true source timestamp; line-status has none (TfL's
`modified`/`created` are stale or .NET-default garbage, so `event_ts =
ingested_at`); disruptions use `lastUpdate` with `ingested_at` as fallback.
Watermarks therefore only do real work on arrivals.

## Decision log (so far)

Each entry: decision, alternatives considered, trade-off. Numbers are from
the data, not estimates; the measurement scripts are in `streaming/`.

### D1. Local compute, cloud only for storage and ingestion
Decision: Kafka, the producer, Spark, Airflow and the dashboard run on one
laptop (i7, 16 GB). Alternatives: EC2 / MSK / Databricks. Trade-off: no
24/7 run (collection is session-based), in exchange for a cloud bill under
£5 and one fewer unknown to fight at a time. Full cost defence in Week 7.

### D2. Kafka between the poller and Spark
Decision: land raw TfL responses in Kafka, not straight into Spark or a
database. Alternatives: poller writes Parquet directly; poller → Postgres.
Trade-off: one more moving part, in exchange for buffering (Spark can be
down while the poller runs), replay (re-run bronze from offset 0 after a
bug) and decoupling (a second consumer costs nothing).
Learned the hard way: **Kafka is a buffer, not a store.** Default topic
retention is 7 days and a 1.5 M-message backlog was deleted before the
first bronze job ran. Retention on `tfl.arrivals` is now 30 days, and the
bronze job's purpose is precisely to copy data out before retention does.

### D3. 11 partitions on tfl.arrivals, keyed by line_id
Decision: key by `line_id` so per-line order is preserved within a
partition; 11 partitions because 11 lines is the most parallelism a
consumer group could ever use. Alternatives: 1 partition (simplest), 3
(arbitrary). Trade-off: hash lumpiness means two partitions are permanently
empty and one is hottest; accepted, because the ceiling costs nothing at
this scale. Kafka orders *within* a partition only; there is no order
between a Victoria and a Jubilee message, and the platform never assumes
one.

### D4. JSON with a versioned schema doc, no Avro / Schema Registry
Decision: JSON messages, contract in `docs/schema.md`, `schema_version` in
every envelope. Alternatives: Avro + Confluent Schema Registry. Trade-off:
no compile-time schema enforcement, in exchange for zero extra
infrastructure. A registry is the right answer once producers and consumers
belong to different teams and evolve independently; here there is one
producer, one consumer and one author, and the version field is the hook
for adding a registry later without breaking anything.

### D5. Explode line-status to one message per line
Decision: TfL returns a nested `lineStatuses` list per line; the producer
emits one flat message per status. Alternative: forward the nested list.
Trade-off: more messages, in exchange for every downstream consumer getting
scalar fields instead of unpacking a list forever.

### D6. Two timestamps in every message
Decision: `event_ts` (source time) and `ingested_at` (pipeline time).
Alternative: one timestamp. Trade-off: eight bytes, in exchange for Spark
being able to window on event time, so late-arriving data lands in the
window it belongs to rather than the window in which it happened to arrive.
Event-time processing is reproducible on replay; processing-time is not.

### D7. Producer dedupe on status and disruptions only, none on arrivals
Decision: a bounded in-memory fingerprint set (sha256 over the payload
minus volatile fields) skips unchanged status/disruption messages; arrivals
are never deduped at the producer because `time_to_station` changes every
poll, so no two are byte-identical. Trade-off: the deduper is a Python set
in RAM; restarting the producer re-sends everything once. That is
acceptable because Kafka's own delivery model is at-least-once, so every
consumer already has to be idempotent. **Dedupe is an optimisation;
idempotent consumers are the correctness guarantee.**

### D8. Fail loudly, retry selectively
Decision: every HTTP call has `timeout=10`; retries only on 429 / 5xx /
network errors with 1, 2, 4 s backoff; 4xx raises immediately. Trade-off:
a poller that dies noisily beats one that hangs silently through a two-hour
unattended run. Retrying a 404 returns the same 404.

### D9. Watermark on arrivals = 2 minutes (measured, not guessed)
Measured over 1,547,523 arrivals (26 Aug evening, no incident):

| skew = ingested_at − event_ts | value |
|---|---|
| min | 0.59 s |
| p50 | 4.5 s |
| avg | 15.6 s |
| p99 | 64.3 s |
| max | 80.8 s |

Decision: `withWatermark("event_ts", "2 minutes")`, above the observed
maximum. Alternatives: p99 (~65 s); the eyeballed "43 s" from Week 2 (which
turned out to be a tail value, not the median). Trade-off is lopsided: a
generous watermark costs seconds of latency; a stingy one silently drops
rows during exactly the incidents this project exists to measure. Caveats:
the sample contains no incident period; revisit after a week of bronze data.

### D10. Bronze partitions by ingested_at, not event_ts
Decision: `data/bronze/arrivals/date=YYYY-MM-DD/hour=HH/` derived from
`ingested_at`. Alternative: partition by TfL's `event_ts`. Trade-off:
bronze answers "when did *we* receive it"; silver answers "when did it
*happen*". Replaying Kafka lands rows in the same bronze folders regardless
of how late TfL's timestamp was, and event-time logic stays where the
watermark lives.

### D11. Dead-letter quarantine instead of crashing
Decision: rows failing the required-field test after `from_json` are
written as raw text to `data/quarantine/arrivals` with their own checkpoint;
good rows go to Parquet. Alternative: `failOnDataLoss`-style crash, or
silent drop. Trade-off: one extra sink and checkpoint, in exchange for bad
data being kept and countable rather than lost. Verified with two
deliberately malformed messages: 1,604,977 good rows, 2 quarantined.

### D12. Exactly-once file output: checkpoint + _spark_metadata
Drill: the bronze job was killed at micro-batch 35 and restarted; it
resumed at 36 and finished at 80 (81 batches of ≤ 20,000 offsets), with
counts unchanged. Two ledgers do two jobs: the **checkpoint** records which
Kafka offsets are done (prevents gaps, sets the restart point);
**_spark_metadata** inside the Parquet path records which files each batch
owns, written only after all files land (prevents duplicates). Only Spark
reads the second ledger, which is a known limitation for Week 7: Snowpipe
and other engines list the folder, so orphan files from a killed batch must
be handled there.

### D13. What "one arrival" is, and the dedupe key (measured)
TfL's Arrivals feed is re-polled every 30 s, so a single approaching train
appears in ~90 consecutive rows. Measured on 1.6 M bronze rows:

- TfL's `id` is stable across polls, but collapses when `vehicle_id` is
  `000` (TfL's "train not identified" placeholder, common at termini): one
  Uxbridge id held 1,760 rows covering several trains × three platforms.
  `000` is 1.06 % of rows.
- One train is often predicted on several platforms at once (platform
  hedging).
- TfL does not regenerate every prediction every 30 s, so consecutive
  polls frequently fetch an identical prediction (same `event_ts`, same
  countdown; only `ingested_at` differs).

Decision, three levels: a **row** (one poll of one board entry) → a
**prediction** = `(line_id, vehicle_id, naptan_id, event_ts)`, the
`dropDuplicatesWithinWatermark` key → an **arrival** = `(line_id,
vehicle_id, naptan_id)`. `ingested_at` is deliberately excluded from the
key (it differs on every row, so including it would make dedupe a no-op).
`platform_name` is excluded (a one-second difference is noise at 5-minute
grain). Rows with `vehicle_id = 000` are excluded from the delay metric and
**counted** as unattributable; that count is itself a data-quality metric.

### D14. What an "actual" arrival is (stated assumption)
The feed contains predictions only; there is no ground truth. Measured:
the lowest countdown ever seen per arrival has p50 = 18 s and p99 =
1,580 s across 16,928 arrivals, so the countdown almost never reaches 0
(the 30 s poll misses the last seconds) and ~1 % of trains vanish while
still 25+ minutes out (cancelled, reversed, lost by tracking, or truncated
because collection stopped).

Decision: an arrival is **completed** when its lowest observed
`time_to_station` ≤ 60 s (two polls); 86.6 % of arrivals qualify. The
rest are counted as not completed, not measured.
**Delay = last `expected_arrival` − first `expected_arrival`** for that
arrival: how far TfL's own forecast drifted between first and last
sighting. Alternative considered: estimate the real arrival instant from
`last ingested_at + last time_to_station`; rejected because it mixes
processing time into the metric and is not reproducible on replay.
Caveat, stated plainly: this is forecast drift, not lateness against a
timetable. There is no timetable in this feed.

### D15. Two jobs for silver, not one
Silver is split in two. Job 1 is the stream: bronze Parquet in, dedupe
within the watermark, session windows, delay, Append to
`data/silver/arrivals` with its own checkpoint. Job 2 is
`window_reliability`, run as a batch over that table.

My reason: every stage gets its own table. When a number looks wrong I can
open the table before it and the table after it and see which stage broke,
instead of debugging inside one long running query.

The honest case for one job: it is the purer streaming design. One
checkpoint, one failure surface, and the 5-minute windows would land
seconds after the arrival finalises instead of waiting for a second job to
run. I know what I gave up. If latency on the windows ever matters more
than debuggability, this decision flips.

### D16. The 12-minute cut-off (measured)
Streaming silver holds **34,179** arrivals. The same six functions run as a
batch over the same bronze give **39,222**. The gap is **5,043 (12.9 %)**:
1,823 completed and 3,220 not completed.

I predicted a few hundred and was wrong. The gap is not "the last session
per key". It is a time cut-off. Append mode writes a session only when the
watermark (newest event minus 2 minutes) is past the session end (last
event plus the 10-minute gap). So any session whose last event is inside
the newest 12 minutes of data is still sitting in state. Proof to the
second: bronze's newest `event_ts` is 19:16:06, silver's newest
`last_event_seen` is 19:03:56, difference 12 min 10 s.

Those rows are not lost. They are in the checkpoint's state store and would
be written the moment one more event moved the watermark. In production the
feed never stops, so the cut-off only bites at the tail of a collection
run.

Options for Week 9, not decided yet:
1. Document it as expected and flag the last 12 minutes of every run in
   the marts (cheapest, honest).
2. Graceful drain: push one synthetic future event through at shutdown so
   the watermark passes everything.
3. Update output mode is off the table: `session_window` does not support
   it (Spark 4.2.0 docs, checked 23 Sep 2026).

### D17. Bronze is what goes to S3 and Snowflake, not silver
Week 4 lands bronze arrivals (every field, as received) in S3 and lets
Snowpipe load it into RAW. Silver tables go to S3 too and dbt reads them
in Week 5; bronze is the one Snowpipe must load because it is the rebuild
point.

Why not silver, since silver is the product: in Week 3 I changed the delay
definition twice and rebuilt silver from bronze each time in minutes. If
only silver had been in the warehouse, every fix would have meant "the
warehouse is wrong and I cannot recompute it". Kafka forgets after 30 days
and a laptop disk is not a warehouse. Bronze in S3 is the copy I can always
rebuild from. It is also what Week 5's dbt staging expects: RAW that still
needs casting and renaming.

How Spark and dbt share the work ("thick Spark"): Spark owns what must be
fresh, reading Kafka and doing the event-time work (watermark, dedupe,
session windows) so silver exists minutes after the train arrives. dbt owns
what must be tested and joined: freshness checks, tests, line-status joins,
hourly and daily marts, built on both RAW (bronze) and Spark's silver. The
one overlap is that bronze gets cast twice, once in Spark for silver and
once in dbt staging. I know that and accept it. Week 10 idea: rebuild
silver's numbers in dbt SQL and check the two engines agree.

### Known limitations (so far)
- Session-based collection: every train still on the board when the
  producer stops looks like a lost train, and the last 12 minutes of every
  run stay in Spark state in Append mode (D16, measured: 5,043 arrivals).
  Handling decided in Week 9.
- Single broker, replication factor 1: fine on one laptop, not a
  production topology.
- Windows host: hadoop.dll / winutils are required for Spark file sinks
  (see NOTES.md for the PATH lesson).

## Numbers so far

| what | value |
|---|---|
| arrivals per 30 s poll (11 lines) | ~3,450 |
| bronze rows written in the Day 2 drain | 1,604,977 (+ 2 quarantined) |
| micro-batches, kill/restart drill | killed at 35, resumed at 36, finished at 80 |
| arrivals skew p50 / p99 / max | 4.5 s / 64.3 s / 80.8 s |
| watermark | 2 minutes |
| distinct arrivals in the Day 3 sample | 16,928 |
| lowest countdown per arrival p50 / p99 | 18 s / 1,580 s |
| completed arrivals (≤ 60 s), before session split (Day 3) | 86.6 % |
| completed arrivals (≤ 60 s), after 10-min session split (Day 4) | 73.6 % |
| `vehicle_id = 000` share | 1.06 % |
| arrivals with 10-min session gap, batch | 39,222 (28,854 completed) |
| 5-minute windows, batch | 13,602 |
| arrivals in streaming silver | 34,179 (27,031 completed) |
| streaming vs batch gap | 5,043 = the 12-minute cut-off |
| silver kill/restart drill | killed after commit 1, resumed at 2, count identical |
| Snowflake trial spend after 9 days (Week 1) | $3.50 |
| total cloud spend to date | TODO |

*TODO (Week 10): total events processed, sessions and hours covered, p95
event-to-warehouse latency, dbt test count, AWS + Snowflake cost breakdown,
disruption lead-time findings.*

## Running it locally (current state)

Prerequisites: Docker Desktop, Python 3.13, JDK 25 (≥ 25.0.3), a TfL API
key in `.env` as `TFL_APP_KEY=...` (gitignored). On Windows, `HADOOP_HOME`
must point at a folder whose `bin/` holds `winutils.exe` and `hadoop.dll`,
and that `bin/` must be on `PATH`.

```powershell
# 1. Kafka
docker compose up -d

# 2. venv
py -3.13 -m venv .venv
.\.venv\Scripts\Activate
pip install -r requirements.txt        # TODO: re-export as UTF-8 and add pyspark==4.2.0

# 3. Topics (first time only)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tfl.arrivals    --partitions 11 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tfl.line-status --partitions 1  --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tfl.disruptions --partitions 1  --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name tfl.arrivals --add-config retention.ms=2592000000

# 4. Producer (leave running)
python -m ingestion.producer.main

# 5. Bronze (second terminal; watch data/checkpoints/bronze_arrivals/commits/ grow)
python streaming/bronze_arrivals.py

# 6. Verify
python streaming/check_bronze.py
```

*TODO: `terraform apply` for the cloud layer (Week 7), `make up` for the
whole stack (Week 8), dashboard (Week 6).*

## Post-mortem

*TODO (Week 10): `docs/postmortem.md`, one incident from the Week 9 live run
written up as timeline, root cause, fix, prevention.*
