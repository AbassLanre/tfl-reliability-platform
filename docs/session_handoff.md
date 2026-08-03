# Session Handoff — TfL Reliability Platform

Paste/attach this into a new Claude session along with the build plan PDF
(tfl_reliability_platform_build_plan_v1_2.pdf). Invoke the
data-engineering-mentor skill first.

## Who I am / how to work with me

- PercyAbs, fairly new to Snowflake, experienced-ish with Python, SQL,
  Postgres, dbt (fintech ETL project), Airflow, Docker, Git.
- Complex things confuse me: teach one small step at a time, verify facts
  rather than guess, use plain-English analogies, and make me do the
  hands-on work myself (agreed work style: "you guide, I type/run").
- I will challenge you on hallucination — verify current facts against
  live docs and say what's verified vs. from memory.
- Machine: Windows, PowerShell. Project folder:
  C:\Users\user\Documents\tfl-reliability-platform (Claude has file access).
- Keep me writing NOTES.md entries for every bug/decision (Week 10 needs it).

## Project status (as of 2026-08-04)

- Week 0 ✅  Week 1 ✅ (checkpoints passed, incl. oral exams)
- Week 2 IN PROGRESS — day 1 done: Kafka up, all six primitives learned
  by experiment, both interview questions (partition-key ordering,
  consumer-group rebalancing) passed orally.

## What happened in the 2026-08-04 session

1. **Deferred Week 1 theory session — DONE (oral-checked):**
   - External vs internal stages (external = signpost to my own S3; files
     stay put = audit trail + disaster recovery + replay/reprocessing).
   - Snowpipe auto-ingest: file lands in S3 → S3 event notification →
     Snowflake-owned SQS queue → Snowpipe runs its stored COPY INTO for
     that file. Serverless from my side; per-file micro-billing; pipe
     load history ~14 days (vs ~64 days for manual COPY INTO).
   - Micro-partitions: immutable columnar chunks + per-column min/max
     metadata → pruning (skip chunks that can't match) instead of indexes.
   - Time Travel: WRITES (not SELECTs — correction drilled) create new
     micro-partitions and retire old ones into a retention window
     (1 day default on Standard); AT(...) / UNDROP query the retired ones.

2. **snow CLI incident (resolved) — NOTES.md material:**
   - `snow` had vanished: it was installed in system Python 3.10, which
     no longer exists on the machine (stale py-launcher entry remains).
   - Reinstalled as standalone Windows MSI binary (no Python dependency)
     from sfc-repo.snowflakecomputing.com. Version 3.23.0.
   - Config-location gotcha: CLI resolves config as SNOWFLAKE_HOME →
     ~/.snowflake (if the dir exists — it does, keys live there) →
     %LOCALAPPDATA%\snowflake. It was reading ~/.snowflake/config.toml
     while I edited the %LOCALAPPDATA% one. Fixed by moving config to
     C:\Users\user\.snowflake\config.toml — single source of truth,
     sits beside the keys.
   - Connection "tfl" now on key-pair auth (authenticator = SNOWFLAKE_JWT
     + private_key_file). `snow connection test -c tfl` → Status OK.
     No passwords anywhere. Old TODO CLOSED.
   - Encoding warning (Windows locale cp1252 vs utf-8): fix chosen =
     [cli.encoding] block in config.toml (file_io/subprocess/stdout =
     "utf-8"). VERIFY warning is actually gone next session.

3. **Credit check:** $3.50 of $400 used (~9 days into trial, activated
   ~2026-07-14, ~21 days left). Week 2 is all-local (no Snowflake spend),
   but don't dawdle before Weeks 4–5.

4. **Week 2 day 1 — Kafka running, primitives learned by experiment:**
   - docker-compose.yml at repo root: apache/kafka:4.3.0 (pinned; version
     verified live against kafka.apache.org quickstart), single broker,
     KRaft, container_name kafka, ports 9092:9092. CLI tools run via
     `docker exec -it kafka /opt/kafka/bin/<tool>.sh`.
   - NOTE: no volume mounted yet — topic data dies with the container.
     Deliberate gap; fix when hardening compose (producer joining / Week 7).
   - Throwaway learning topics created: test-events (1 partition),
     group-demo / group-demo2 (3 partitions).
   - Seen first-hand: topic / partition / offset (incl. consuming from an
     arbitrary offset with --partition 0 --offset 2, and using
     kafka-get-offsets.sh to check end offsets when a read returned
     nothing); console producer/consumer; consumer group with two members.
   - Surprise worth keeping: KEYLESS messages are sticky-batched (~16KB)
     to ONE partition — one consumer got everything. KEYED messages
     (parse.key=true, key.separator=:) route by hash(key) % partitions —
     per-key order preserved, NO global/cross-key ordering.
   - Rebalance drill: killed one consumer mid-stream; survivor inherited
     its partition and resumed from the last COMMITTED offset (no replay
     of already-processed messages). Delivery model = at-least-once
     (messages processed after last commit but before the crash get
     reprocessed) → this is Week 3's dedup/watermark motivation.

## Snowflake account state

- Trial: Standard / AWS / eu-west-2 (London), $400 credits, 30 days from
  ~2026-07-14. Chosen to match Week 7 S3 region.
- Account WBGCTLM-QP79037 (locator KU42338), user PERCYABS (password auth
  has enforced TOTP MFA; key-pair auth used by all clients), role
  ACCOUNTADMIN (known shortcut — proper roles deferred to Week 4).
- Warehouse TFL_DEV_WH (X-Small, AUTO_SUSPEND=60, AUTO_RESUME; all trial
  warehouses on AUTO_SUSPEND=60; ALLOW_CLIENT_MFA_CACHING=TRUE set;
  user default warehouse = TFL_DEV_WH).
- DB TFL_DEV: RAW (CSV_STANDARD file format, CSV_STAGE internal stage,
  TUBE_LINES 12 rows via PUT + COPY INTO) and STAGING
  (STG_TUBE_LINES view built by dbt). dbt debug + build pass.

## Auth setup

- RSA keys at C:\Users\user\.snowflake\keys\snowflake_key.p8 / .pub
  (OUTSIDE repo). Public key registered via ALTER USER ... SET
  RSA_PUBLIC_KEY. Generator script: scripts/generate_snowflake_key.py
- dbt/profiles.yml (lives IN the dbt folder, not ~/.dbt) uses
  private_key_path. No passwords in any file.
- snow CLI 3.23.0 (standalone MSI binary), config at
  C:\Users\user\.snowflake\config.toml, connection "tfl", key-pair auth.

## Local environment

- Python 3.13 venv at .venv/ in repo root, dbt-snowflake ONLY.
  Python 3.10 is GONE from the machine (stale py-launcher entry) — that's
  what killed the old pip-installed snow CLI. Rule reaffirmed: project
  libraries in venv, standalone CLI tools as isolated binaries (MSI/pipx),
  recipe in requirements.txt (committed), .venv gitignored.
- Past incident (still relevant): snowflake-cli inside the venv once
  downgraded protobuf/click and broke dbt-core constraints; pins restored
  (protobuf>=6<7, click>=8.3<9). `pip check` clean.
- Docker Desktop 29.5.3 / Compose v5.1.4. The fintech Airflow stack
  (7 containers) tends to be left running — habit adopted: `docker compose
  down` stacks not in use before Kafka/Spark sessions.
- Repo also has: docs/sample_data/, docs/sample_payloads/
  (⚠ currently ONLY statusByMode.json), snowflake/ SQL, NOTES.md.

## Next session agenda (Week 2 continues)

1. Quick verifies: snow CLI encoding warning gone; Kafka container up
   (`docker compose up -d` recreates it — learning topics will be gone,
   that's expected and fine).
2. **Fetch missing sample payloads first**: docs/sample_payloads/ has only
   statusByMode.json — need /Line/{id}/Arrivals (and disruptions) samples.
   TfL API key exists from Week 0.
3. **Design the event schema** (~half day): message formats for
   tfl.arrivals, tfl.line-status, tfl.disruptions → docs/schema.md.
   Must include: TfL timestamp (future Spark event time), ingestion
   timestamp (processing time), line, station, platform, prediction fields.
4. Create the three real topics with deliberate partition counts
   (decision now understood: partition = ordering boundary + parallelism
   cap; producer keys by line_id).
5. Start the **Python producer** (2–3 days): poll TfL API (30–60s
   arrivals, 60s status), backoff/retry, dedupe obvious repeats, JSON to
   Kafka keyed by line_id; proper package (config, logging), containerised.
   This is the code interviewers actually read. Producer deps go in the
   project venv per the venv rule. Kafka client library choice: verify
   current recommendation live (confluent-kafka vs kafka-python-ng etc.)
   before installing.
6. Housekeeping owed: NOTES.md entries from this session (snow CLI death,
   config resolution order, encoding/locale fix, sticky partitioner,
   rebalance drill, at-least-once); commit docker-compose.yml;
   fix typo terraform/placrholder.py → placeholder.py.

## Week 2 "done when" (checkpoint to hit before Week 3)

Producer runs 2+ hours unattended; thousands of real events in three
topics; can consume from an arbitrary offset; can explain partition-key
ordering and consumer-group rebalancing without notes (already passed —
re-test at checkpoint). Pitfall to respect: no Avro/Schema Registry —
JSON, with the skip defended in the README.

## Mentor conventions to keep

- Verify current facts (versions, docs, auth policies) against live docs;
  say what's verified vs. from memory. (Kafka image version and Snowflake
  CLI install method were both verified live this session — keep it up.)
- Every week ends with the plan's "done when" checkpoint including an
  oral exam without notes.
- Hints before answers on learning exercises; escalate if stuck.
- Time-box yak-shaves; always leave a working fallback path.
- Celebrate error messages as material: every failure becomes a NOTES.md
  line and an interview story.
- One small step at a time; Percy types/runs everything himself.
