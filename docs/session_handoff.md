# Session Handoff — TfL Reliability Platform

Paste this into a new Claude session along with the build plan PDF
(tfl_reliability_platform_build_plan_v1_2.pdf). Invoke the
data-engineering-mentor skill first.

## Who I am / how to work with me

- PercyAbs, fairly new to Snowflake, experienced-ish with Python, SQL,
  Postgres, dbt (fintech ETL project), Airflow, Docker, Git.
- Complex things confuse me: teach one small step at a time, verify facts
  rather than guess, use plain-English analogies, and make me do the
  hands-on work myself (agreed work style: "you guide, I type/run").
- Machine: Windows, PowerShell. Project folder:
  C:\Users\user\Documents\tfl-reliability-platform (Claude has file access).
- Keep me writing NOTES.md entries for every bug/decision (Week 10 needs it).

## Project status: Week 0 ✅ and Week 1 ✅ COMPLETE (as of 2026-07-16)

Week 1 checkpoint passed: CSV loaded via stage is queryable; dbt model
builds against Snowflake from my machine; oral exam on storage/compute
separation passed (with corrections noted below).

## Snowflake account state

- Trial: Standard edition / AWS / eu-west-2 (London), $400 credits,
  30 days from ~2026-07-14. Chosen to match Week 7 S3 region.
- Account identifier: WBGCTLM-QP79037  (locator KU42338, eu-west-2)
- User: PERCYABS (email+password with enforced TOTP MFA), role ACCOUNTADMIN
  (known shortcut — proper roles deferred to Week 4).
- Objects created:
  - Warehouse TFL_DEV_WH (X-Small, AUTO_SUSPEND=60, AUTO_RESUME).
    Pre-existing trial warehouses all set to AUTO_SUSPEND=60 too.
    ALTER ACCOUNT SET ALLOW_CLIENT_MFA_CACHING = TRUE was run.
    User default warehouse = TFL_DEV_WH.
  - Database TFL_DEV, schemas RAW (mine) and STAGING (created by dbt).
  - TFL_DEV.RAW: file format CSV_STANDARD, internal stage CSV_STAGE,
    table TUBE_LINES (12 rows, loaded via PUT + COPY INTO).
  - TFL_DEV.STAGING.STG_TUBE_LINES view built by dbt.

## Auth setup (the hard-won part)

- Password auth from clients fails without a TOTP passcode (account policy).
- Solution in place: key-pair auth. RSA keys at
  C:\Users\user\.snowflake\keys\snowflake_key.p8 / .pub (OUTSIDE repo).
  Public key registered via ALTER USER ... SET RSA_PUBLIC_KEY.
  Generator script: scripts/generate_snowflake_key.py
- dbt/profiles.yml (lives IN the dbt folder, not ~/.dbt) uses
  private_key_path. No passwords in any file.
- snow CLI (installed in SYSTEM Python 3.10, deliberately NOT in venv):
  config at C:\Users\user\AppData\Local\snowflake\config.toml, connection
  name "tfl". STILL TODO: switch it from password to
  private_key_file + authenticator = "SNOWFLAKE_JWT".

## Local environment

- Python 3.13 venv at .venv/ in repo root (recreated after 3.10 caused
  pip resolution-too-deep). Contains dbt-snowflake ONLY (project deps).
  Rule adopted: libraries in venv, standalone CLI tools via pipx/system,
  recipe in requirements.txt (committed), .venv gitignored.
- Known incident: installing snowflake-cli into the venv downgraded
  protobuf/click and broke dbt-core constraints; it was uninstalled and
  pins restored (protobuf>=6<7, click>=8.3<9). `pip check` clean.
- dbt project: dbt/ (dbt_project.yml, profiles.yml,
  models/staging/{sources.yml, stg_tube_lines.sql}). dbt debug + build pass.
- Repo also has: docs/sample_data/tube_lines.csv, docs/sample_payloads/,
  snowflake/week1_01_setup.sql, NOTES.md (running log).

## Concepts learned (verified by oral exam)

Storage/compute separation; virtual warehouses (correction drilled: a VW
is compute only, stores no data — the name is misleading); stages =
loading dock for files (internal vs external); COPY INTO load metadata /
idempotency (tracks files ~64 days, skips already-loaded files — protects
against re-runs, not duplicate data in renamed files); why PUT must run
from a local client, not Snowsight; env vars for secrets (password was
once pasted in chat → rotated; never again); venv discipline.

## Outstanding before/at start of next session

1. 30-min theory session still owed (deferred from Week 1): external
   stages vs internal, Snowpipe auto-ingest (classic, from S3 — the Week 4
   mechanism), micro-partitions and time travel at "know what these are"
   depth. Do this BEFORE starting Week 2.
2. Switch snow CLI config to key-pair (5 min).
3. Check credits consumed in Admin → Cost Management, log in NOTES.md
   (weekly habit).
4. Then: Week 2 — Kafka in Docker (KRaft), six primitives, TfL producer.
   TfL API key exists from Week 0; sample payloads in docs/sample_payloads/.

## Mentor conventions to keep

- Verify current facts (trial terms, tool versions, auth policies) rather
  than trusting training data; say so when unverified.
- Every week ends with the plan's "done when" checkpoint including an
  oral exam without notes.
- Time-box yak-shaves; always leave a working fallback path.
- Celebrate error messages as material: every failure becomes a NOTES.md
  line and an interview story.
