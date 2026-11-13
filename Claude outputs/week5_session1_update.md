# Week 5 session 1 (13 Nov 2026, afternoon): paste-in updates

The folder link was down at the end of the session, so these are ready to paste.
Percy: review, paste, commit.

---

## 1. TASKS.md: replace the Week 5 block with this

```
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
```

---

## 2. docs/to_know.md: add at the end

```
## Week 5 session 1 (13 Nov): dbt staging + first intermediate model

- Source freshness: dbt checks the newest snowflake_loaded_at; warn after 30 min,
  error after 2 h. Red today is CORRECT: nothing has streamed since 28 Oct.
- YAML needs a space after the colon. `{count:30}` silently became nothing.
- Seed = small CSV inside the dbt project, loaded by dbt. Right for reference data
  (tube lines). seeds are ref()'d, not source()'d.
- Staging = open the envelope and label the pigeonholes: cast and rename only, row count
  must equal RAW (1,624,954).
- D20 test fired on real data: 4 duplicates, 30 s apart on one line -> TfL repeating an
  entry, not a double-loaded file. Alarm tuned: warn on small counts, error above 100.
- D21 = B: silver rebuilt in dbt. One tested source of truth in the warehouse; Spark is
  the live engine; Week 10 compares them.
- WHERE runs before window functions, so you cannot filter on row_number() in WHERE.
  Use a CTE (works everywhere) or QUALIFY (Snowflake/BigQuery/Databricks).
- to_varchar(timestamp) defaults to milliseconds. My key collapsed 55 sub-ms pairs; the
  unique test caught it. Build keys at the same precision as the dedupe.

**STAR: the duplicate detector**
S: Snowpipe can't see Spark's commit log, so a file could be loaded twice.
T: detect that without silently hiding it.
A: fingerprinted every raw row in dbt and put a uniqueness test on it.
R: first run found 4 real duplicates from TfL's API, which also proved no file had
been double-loaded; set it to warn for small counts and error above 100.
```

---

## 3. docs/session_handoff.md: replace the top of "NEXT (start here)" with

```
### NEXT (start here): Week 5, step 6.2 int_arrivals

Done 13 Nov (session 1 of Week 5): 5.1-5.5 + 6.1, see TASKS.md. dbt state:
seeds/tube_lines.csv; staging/sources.yml (raw.arrivals with freshness under config:);
stg_tube_lines, stg_arrivals (+12 tests, fingerprint warn>0/error>100);
intermediate/int_arrival_predictions (table, 1,068,808 rows, key with FF9).
dbt 1.11: generic test args go under `arguments:`; use `data_tests:`.

Next: mentor READS streaming/silver_arrivals.py first (session gap logic, completed
rule lowest_tts <= 60, delay definition) and mirrors it in SQL as int_arrivals, so
Week 10 "engines agree" compares like with like. Then 5-min windows, then marts.
Pace: Percy is short on time (school). Full code + plain explanation + prediction,
one step per message, no detours.
```
