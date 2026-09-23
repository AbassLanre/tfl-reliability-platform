7/13/2026
CPU - Intel core i7 9th gen, 2.60ghz
RAM - 16gb
Free space - 125gb


Postgres couples storage and compute on one machine, so you scale them together whether you need to or not. Snowflake stores data once in cheap object storage and runs stateless compute clusters against it on demand, billed per second — so you scale compute independently, run isolated workloads on the same data, and pay nothing when idle

 Postgres couples everything on one machine; Snowflake keeps data in cloud object storage. The half you're missing: you never said virtual warehouse. "Queries are done on Snowflake" is where an interviewer leans in and asks "on what, exactly?" The answer: on a warehouse — a named, sized compute cluster you create (TFL_DEV_WH, X-Small), that wakes on demand, bills per second while running, sleeps when idle, and — crucially — can exist in multiples: dbt on one warehouse, dashboards on another, same data, zero contention. That's the sentence that separates "used Snowflake" from "understood it". Also one small correction: Postgres isn't defined by being your machine — a company Postgres runs on a big server somewhere; the point is storage and compute are welded together on whichever machine that is, so they scale together whether you like it or not.

 Internal vs external, storage locations, S3 — all right. One refinement: a stage isn't about small data, it's about files in transit — the loading dock where files wait before COPY INTO moves them into tables, at any scale. And an external stage doesn't hold anything itself; it's a pointer to your bucket. In Week 4, terabytes could flow through one.

 Snowflake remembers loaded files (~64 days of load metadata) and skips them to prevent duplicates. One word wrong, and precision matters here: nothing is blocked from the stage — your file is still sitting in the stage right now, LIST would show it. What's skipped is the copy into the table. Stage = dock, table = warehouse floor; the second lorry was turned away at the floor, not the dock. Bonus detail worth keeping: it tracks files, not rows — same data in a differently-named file would load, and create duplicates. The idempotency protects against re-running, not against you.

 A virtual warehouse is a cluster of compute resources that executes queries against data it doesn't store — you size it, it bills per second while running, suspends when idle, and you can run several at once against the same data
 
WBGCTLM-QP79037.snowflakecomputing.com
Activation date - 7/13/2026
standard edition
chose eu west 2 cus same region as I, keeps the storage-integration handshake simple and transfer costs at zero

creating the warehouse

XSMALL — the smallest compute cluster, 1 credit/hour when running. Everything in this project runs on X-Small; bigger sizes just burn trial credits faster for no benefit at your data volumes.
AUTO_SUSPEND = 60 — that's seconds of idle time before it switches itself off. This is the pitfall line: the default is 600, and a warehouse without it burns credits doing literally nothing.
AUTO_RESUME = TRUE — it wakes automatically the next time a query needs it. You never manually start/stop.
INITIALLY_SUSPENDED = TRUE — create it off, so the billing clock doesn't start until your first query.

suspended warehouses cost zero, running ones burn credits — so extra warehouses are harmless, but any warehouse with a long auto-suspend is a slow leak

using the sample doc tube_line.csv, snowflake gave it an auto comuln called c1,c2, c3 and i was able to load a column from that

after trying to install snow cli on my system, initially got pip erro, had to reinstall pip
then got connection to tfl not configured error

issues with snowflake connection addition, had to fill 

Enter connection name: tfl
Enter account: 
Enter user: PERCYABS
Enter password:
Enter role:
Enter warehouse:
Enter database:
Enter schema:
Enter host:
Enter port:
Enter protocol:
Enter region:
Enter authenticator: externalbrowser
Enter workload identity provider:
Enter private key file:
Enter token file path:
Enter secondary roles:


never paste passwords in cli lmao unless prompted
snowflake knows which data it already loaded, loading metadata

Question: how does development, staging and production look like as a data engineer

initially we shouldnt use accountadmin as our account but we'd fix that later

venv activated but bypassed with a full path; Python 3.10 → resolver meltdown → upgraded to 3.13.

Installed snowflake-cli into the dbt venv; it downgraded protobuf/click and broke dbt-core's constraints. Fix: CLI tools live outside the project venv (pipx / system), project venv holds only project dependencies. pip check verifies

proper steps

# 1. Project folder + git
mkdir C:\Users\user\Documents\my-new-project
cd C:\Users\user\Documents\my-new-project
git init

# 2. Venv INSIDE the project (once per project)
py -3.13 -m venv .venv

# 3. Activate (every session) — prompt shows (.venv)
.\.venv\Scripts\Activate

# 4. Sanity check — which Python is answering? Must end in .venv\Scripts\python.exe
python -c "import sys; print(sys.executable)"

# 5. Install the project's libraries into the venv
python -m pip install --upgrade pip
pip install dbt-snowflake        # or dbt-postgres, whatever the warehouse is

# 6. Freeze what you installed, commit the recipe (not the venv itself)
pip freeze > requirements.txt
# .venv/ goes in .gitignore; requirements.txt goes in git

then 

$env:SNOWFLAKE_PASSWORD = ""
 cd dbt
  dbt debug

  the above env password stuff didn't work as dbt can't access mfa passkey, so we had to switch to generating public and private passkeys

  https://datacoves.com/post/dbt-snowflake (covers this)

  but for windows i basically got AI to write up a script that generates it instead of going through the hassle of openssl

  in profiles.yml (password and MFA lines replaced with private_key_path)

  then after that ran: python scripts\generate_snowflake_key.py
which created the .snowflake passkeys in Users/user/.snowflake/....

so now i also updated config.toml with : private_key_file = "C:/Users/user/.snowflake/keys/snowflake_key.p8" and authenticator = "SNOWFLAKE_JWT"

so passwords don't get asked

week 2
audit trail- tracing a number or field that looks wrong from your snowflake to s3 bucket
disaster recovery - get back your data after initial corruption in your snowflake
reprocessing with future logic- 

External stage = signpost to my own S3; files stay put = audit + recovery + replay
snowpipe is a copy into statement that runs automatically

It's serverless from your point of view
It keeps load history per pipe

first the file lands in the s3 bucket then an event notification is sent saying tht a file has landed, snowflake listens to the queue and sees the message then runs the copy into for that file
Bucket → notification → queue → Snowpipe runs COPY INTO
snowflake avoids opening boxes that literally don't have what we need
immutable columnar chunks + min/max metadata + pruning.

Micro partition and Time travel - writes create new micro-partitions and retire old ones; the retired ones sit in the retention window; Time Travel is just querying those retired chunks

S3 event → SQS → auto COPY INTO

micro-partition pruning, and Time Travel = retired partitions in retention.

snow CLI vanished with uninstalled Py3.10; reinstalled as standalone binary so it can't happen again

the CLI dying with the uninstalled Python 3.10 (reinstalled as standalone binary), the config-location resolution order (edited the right file in the wrong place), and the locale/encoding warning (cp1252 vs utf-8, fixed in config). Each one is symptom → diagnosis → fix, a line or two apiece.

$3.5 after 9 days, auto spend discipline workinggg

A broker is just the Kafka server process — the thing that receives messages, writes them to disk, and serves them to readers

"KRaft replaced ZooKeeper for cluster metadata

close a docker container:

docker compose -f C:\Users\user\Documents\fintech_airflow\docker-compose.yaml down

we use docker exec to run commands on the cli for the kafka:

create topic:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic test-events --bootstrap-server localhost:9092

describe: 
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic test-events --bootstrap-server localhost:9092

Topic: test-events      TopicId: tDyGFD2hQuS89f9qzMIdqA PartitionCount: 1       ReplicationFactor: 1    Configs: min.insync.replicas=1,segment.bytes=1073741824
        Topic: test-events      Partition: 0    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:


Topic: the plan's analogy (named category of messages e.g. tfl.disruptions, tfl.arrivals, tfl.line-status)

Partition: Topics are split into numbered logs called partition, each partition is append only i.e each new message get appended to the bottom. Partitions are Kafka's unit of parallelism and ordering

Replication factor: 1: each partition can be copied across several brokers for safety. in prod replication factor gets taken to 3

to type messages in:
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic test-events --bootstrap-server localhost:9092

to read message:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic test-events --from-beginning --bootstrap-server localhost:9092

the broker is a logbook not a queue so when a message is read, it does not get destroyed after

Every message in a partition gets a sequential number, forever, offset

a consumer group is a team of consumers sharing one name (--group whatever), and Kafka enforces a rule — within a group, each partition is served to exactly one consumer

The point is parallelism without duplication: three consumers in a group on a three-partition topic each handle a third of the traffic, and no message is processed twice by the team

no partition is left unread, kafka must serve every partition to someone in the group

keyless messages are sticky-batched to one partition — saw one consumer get everything.

a worker dies, the team absorbs its workload automatically.

rebalance — another consumer picks up its partitions from the last committed offset

Every Kafka topic has a retention policy. The default, inherited from the broker's log.retention.hours=168, is 7 days, Once a chunk of the log (a segment file) is older than that, the broker deletes it and moves the earliest offset forward

the tfl producer key messages by line_id so the messages are grouped according to the key and preserves per-line order within the same partition
no data is lost when a consumer dies because of rebalancing, another consumer picks up the partition from the last commited offset

in week 3
kept mode_name in the schema cus i wanted to leave it for future use case

Ordering. Messages with the same key always land in the same partition (hash(line_id) % N), so events for the victoria line stay in order no matter what N is. Ordering doesn't force your choice here.
Parallelism cap. One partition can be read by at most one consumer in a group — you watched this in the rebalance drill. So N = the maximum number of consumers that could ever share the work. N=1 means one reader forever; N=6 means up to six.

Supermarket analogy: partitions are checkout lanes. The same customer always joins the same lane (that's the key), so their items stay in order. More lanes means more cashiers could work at once — but lanes nobody queues in are just floor space you're paying for.

venv = isolates what pip install puts in; Docker = isolates what docker run starts up. Neither contains the other.

status - 1 partition count (the status wouldn't change as much within a short time period)
arrivals - 11 partition count (because 11 distinct keys means 11 partitions is the most parallelism that could ever be useful, one consumer per line's worth of traffic, so I set the ceiling at the theoretical max, which costs nothing at this scale)
disruptions -1 partition count (here also there wouldn't be much change)

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tfl.arrivals --partitions 11 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tfl.line-status --partitions 1 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic tfl.disruptions --partitions 1 --replication-factor 1

Topic: tfl.disruptions  TopicId: qbHVus_-QCOJgDIlTXfsZQ PartitionCount: 1       ReplicationFactor: 1    Configs: min.insync.replicas=1,segment.bytes=1073741824
        Topic: tfl.disruptions  Partition: 0    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
Topic: tfl.arrivals     TopicId: IZ0cZwPwSuO6gkiHC_2cWw PartitionCount: 11      ReplicationFactor: 1    Configs: min.insync.replicas=1,segment.bytes=1073741824
        Topic: tfl.arrivals     Partition: 0    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 1    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 2    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 3    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 4    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 5    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 6    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 7    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 8    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 9    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
        Topic: tfl.arrivals     Partition: 10   Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:
Topic: tfl.line-status  TopicId: QEJ850afSUmhfv5FLhZz4A PartitionCount: 1       ReplicationFactor: 1    Configs: min.insync.replicas=1,segment.bytes=1073741824
        Topic: tfl.line-status  Partition: 0    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:

killed docker and tried restarting it after saving the volume info into docker-compose.yml but it failed to start becuse of permisiion issues, so had to set appuser to uid 1000

when testing the config.py file, i had to run it in venv
python -c "from ingestion.producer import config; print(config.KAFKA_BOOTSTRAP, len(config.TUBE_LINES))"

1. Timeouts are not optional. requests.get() with no timeout will wait forever — if TfL stops responding mid-connection, your producer just silently freezes, and tonight's 2-hour unattended run collects nothing. Every request gets timeout=10. A producer that dies noisily beats one that hangs quietly — this is the same fail-fast philosophy as your config.

2. Not every error deserves a retry. Sort responses into three buckets: success (2xx) — return the data; transient trouble (429 "slow down", or any 5xx server error, or a network drop) — worth retrying, because it'll probably pass; and our own fault (404, 401 — wrong URL, bad key) — retrying identical junk gets identical junk, so raise immediately and let a human fix it. Retrying a 404 is one of the most common junior mistakes in ingestion code.

3. Back off exponentially. When you retry, wait 1s, then 2s, then 4s. Hammering a struggling server at full speed makes you part of its problem — and with a 429 it can get your key throttled harder. Doubling the gap gives the other side room to recover. (Production systems add random jitter so a thousand clients don't all retry in sync — say that in an interview and smile; with one laptop you don't need it.)

# tfl.arrivals table. raw["..."] if required, raw.get("...") if optional.

tried running python scripts\test_transform.py in the terminal butit said no module called ingestion found, this is because it was looking for a module in scripts\....

fix is to use python -m scripts.test_transform

So the first p.poll(1) doesn't "wake" anything — the messages were already gone; it just ran the confirmations that had accumulated. Distinction to keep: I/O happens on their thread; callbacks happen on yours.

(.venv) PS C:\Users\user\Documents\tfl-reliability-platform> python -m ingestion.producer.main
2026-09-01 13:12:32,735 INFO producer: arrivals: produced 3451
2026-09-01 13:12:32,802 INFO producer: line status: produced 14
2026-09-01 13:12:32,803 INFO producer: line status: skipped 0
2026-09-01 13:12:33,764 INFO producer: disruptions: produced 8
2026-09-01 13:12:33,765 INFO producer: disruptions: skipped 0
2026-09-01 13:13:03,679 INFO producer: arrivals: produced 3475
2026-09-01 13:13:31,157 INFO producer: line status: produced 0
2026-09-01 13:13:31,158 INFO producer: line status: skipped 14
2026-09-01 13:13:32,380 INFO producer: disruptions: produced 0
2026-09-01 13:13:32,381 INFO producer: disruptions: skipped 8

Arrivals offset:
(.venv) PS C:\Users\user\Documents\tfl-reliability-platform> docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --topic tfl.arrivals --bootstrap-server localhost:9092
tfl.arrivals:0:6333
tfl.arrivals:1:10164
tfl.arrivals:10:20665
tfl.arrivals:2:4861
tfl.arrivals:3:0
tfl.arrivals:4:12761
tfl.arrivals:5:3876
tfl.arrivals:6:76
tfl.arrivals:7:2081
tfl.arrivals:8:4018
tfl.arrivals:9:0

line-status offset:
(.venv) PS C:\Users\user\Documents\tfl-reliability-platform> docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --topic tfl.line-status --bootstrap-server localhost:9092
tfl.line-status:0:91

disruptions offset:
(.venv) PS C:\Users\user\Documents\tfl-reliability-platform> docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --topic tfl.disruptions --bootstrap-server localhost:9092
tfl.disruptions:0:45

1. Ordering (re-test). Your producer keys every message by line_id. Explain precisely what ordering Kafka guarantees for your arrivals data — and just as precisely, what it does not guarantee. Where does the guarantee physically come from?

Q1 — half marks. The mechanism is right: hash(key) % N picks the partition, and within a partition order is preserved. But I asked for what's not guaranteed too, and that half is where interviews are won: there is no ordering across partitions — a Victoria message and a Jubilee message have no defined order relative to each other, and even two keys that share a partition interleave without cross-key promises. And the "where does it physically come from" bit: each partition is an append-only log with a single leader broker appending in arrival order — order isn't computed, it's just the order things were written down. Say the negative half unprompted next time; it's what separates "used Kafka" from "understands Kafka".

2. Rebalancing and delivery (re-test, extended). In Week 3, a Spark consumer group will read tfl.arrivals. One consumer crashes mid-batch — walk me through what happens: who takes over, where do they resume from, and which messages (if any) get processed twice? Name the delivery model.

Q2 — half marks. Takeover ✓, resume from last committed offset ✓. But the two-part sting was the point: messages the dead consumer processed but hadn't committed get processed again by the survivor — and the model has a name you should say out loud: at-least-once delivery. You had this cold three weeks ago in the rebalance drill; it's rusted slightly. It matters because it's the setup for Q3..

3. This week's material. Your deduper is a Python set in memory. Tomorrow morning you restart the producer. What happens on its first status poll after the restart, and why is that acceptable — what property must downstream consumers have anyway, and what single Kafka fact makes that property non-negotiable regardless of your dedupe?

Q3 — no marks, so let's fix it properly. "A confirmation is sent" — no, nothing of the sort exists; that was a guess, and in an interview a guess dressed as an answer costs more than "I'm not sure, let me reason it out". Reason it out now: the deduper's memory is a Python set. Where does a Python set live? In the process's RAM. Restart the process → RAM gone → the set is empty, exactly like the very first run. So the first status poll after restart matches nothing, and all 14 statuses get re-sent as duplicates into the topic. Why is that acceptable? Because your consumers already can't assume no-duplicates — Q2 just established that Kafka itself re-delivers on crash-and-resume. At-least-once is the contract of the whole pipeline; your dedupe only reduces duplicates, it can't eliminate them, so downstream must be idempotent — able to see the same event twice and produce the same result. One sentence ties it together: "dedupe is an optimisation, idempotent consumers are the correctness guarantee." That's also precisely why the crude clear() in your Deduper was acceptable — re-sends are already survivable by design.

4. Design defence. An interviewer looks at your repo and asks: "You've got two timestamps in every message and you split one TfL status response into multiple Kafka messages. Why?" Defend both decisions in under a minute each.

Q4 — pass. Both timestamps named correctly; grain reason correct. Sharpen each with its consequence for the marks you dropped: two timestamps because Spark windows on event time, and late-arriving data would land in the wrong window on ingestion time; exploded grain so every consumer gets flat scalar fields instead of unpacking a list forever.


Spark:

It has 4 ideas in it
-- Event time vs Processing time
event time is the time from tfl itself, the time a event happened e.g event_ts
processing time is the time it landed at the receiving end e. ingested_at
event time is reproducible while processing time isn't

in our project, event time quality differs per topic, arrival has the real/true event time from tfl line-status has none so event_ts=ingested_at and disruptions is lastUpdate or fall back to now

-- Idea 2 Watermarks - deadline
you set the deadline and not rely solely on the event_ts
watermark = latest event_ts seen - delay
watermark delay n quantities that data less than N late will not be aggregated

you will have to measure the distribution before you can give a delay value
watermark delay is a measured decision and not a guess, typical skew is 43 secs but the tail is unknown so we have to measure p99

watermark      = newest event_ts seen so far  −  delay
window closes  when  watermark ≥ window end
event dropped  if it belongs to a window that has already closed

in my own words I'd say watermark is the newest event time  seen after you take away the chosen delay  i.e a window of 8:10-8:20 having a 5 minute water mark and the last seen event was 8:23, with the 5 minute watermark, you have 8:18 which falls within the window therefore the window is not closed and if an event of 8:19comes, it won't get dropped but if the last seen was 8:26 and a 8:15 event comes, it'd be dropped because the watermark has passed the window

Without a watermark, Spark has to keep every window in memory forever in case a late event turns up. The watermark is the deadline that lets it throw old windows away

newest seen   = 08:06
watermark     = 08:06 − 3 min = 08:03
window end    = 08:05
08:03 > 08:05?   No  →  window still open
08:04 arrives  →  belongs to an open window  →  COUNTED


-- idea 3
Window - quick one
basically 8:00-8:05
two types:
Tumbling - fixed, back to back e.g 8:00-8:05, 8:05-8:10....
sliding - overlapping e.g 8:00-8:10, 8:05-8:15, 8:10:8:20....

-- idea 4
Micro batches
This is about how often
spark does not process event one at a time as the event arrives but instead runs a loop

-Wait for a trigger (say, every 10 seconds).
-Grab everything that arrived in Kafka since last time — that's one micro-batch.
-Process that batch as if it were a small ordinary DataFrame.
-Write results, save progress, go back to 1.

it runs forever, basically a batch that runs forever in small chunks. latency is bound by trigger interval.

-- In one sentence each: what is event time, what is processing time, and which one gives reproducible results on replay?
-- Explain watermarks to me as if I'm a junior who's never heard the term. Use a concrete example with real times. Aim for the length of a two-minute spoken answer — roughly a paragraph.
-- Why can't the watermark just use the wall clock instead of the newest event time seen?
-- Tumbling vs sliding windows, one line each, and which one you're using and why.
-- "Is Spark true streaming?" — answer it the way you'd answer an interviewer.
-- Bonus, the sting: it's 3am, the tube is closed, no events are arriving. What happens to your watermark and your open windows?

1. event time is the time from source i.e the time recorded by tfl, processing time is the time it lands, the time the producer polled it from the api, only the event time is reproducible
2. Watermark is basically just the time from when the newest last seen event minuses the set delay period, if i have a delay of 5 min, and a window of 8:20-8:30, and last seen of 8:35, and an event of 8:29 comes, i get a watermark of 8:35-5 min which is 8:30 and this watermark has not passed the end so the window doesn't close and 8:29 doesn't get dropped but if the last seen event was 8:36, then the window will get closed and 8:29 gets dropped
3. the watermark can't use the wall clock because we need o be able to reproduce the result
4. Tumbling is fixed or back to back and sliding is overlapping, we are using tumbling because it doesn't overlap but addup for when we work hourly, a 12 minutes window sum into one hour.
5. spark is not exactly a true streaming as it runs a loop, micro batches whose latency is dependent on the trigger interval
6. as events are not arriving each window will still not get an event and remain open and the process will keep running, i imagine the last seen could be 12:00am and we made windows of 5 min delay and its currently in the window of 12:30-1240, 11:55pm doesn't land in the window and remain open until a last seen causes the watermark to fall outside the window and closes it

SPARK

First we do a batch call to decide the watermark, we check the timestamps and subtract them. count of arrivals - 1547526 (total)

If %TEMP% fills up over the week, delete the spark-* folders by hand.
had issues with winutils and had to go to the github repo to download the twoo files, hadoop.dll and winutils.exe and placed them in C:\hadoop\bin
Spark displays timestamps in session TZ; pinned to UTC to match producer
 this sample contains no incident period, so the tail during a real disruption is unmeasured; and revisit once the bronze job has a week of data on disk

 ran the watermark test to see what delay value we can use as our watermark, min was 0.59s and max was about 81 secs, p50 was 4.5s as opposed to the 43 secs we thought would be our median, and p99 is 64s

 the watermark would best be used as 2 minut3es to cover even further disruptions that could bypass the 81secs max, its safer 

 tried to run bronze.arrivals stream but faced an issue with hadoop.dll as it failed to load
 a warning I had classified as harmless turned fatal the first time a different code path ran; lesson is that a warning about a missing component is only harmless until something needs the component

 fixed this by adding the hadoop/bin to the user file path

 I started working on spark 12/13 days after the broker got the data, but i missed that kafka has a default retention period, 7 days, kafka is a buffer and not a store, the bronze layer is to get the data or rather copy the data out of the kafka to a durable storage location before the retention period ends

 updated the retention.ms to 30 days

 docker exec -it kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name tfl.arrivals --add-config retention.ms=2592000000

 then check again with

 docker exec -it kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics --entity-name tfl.arrivals

 1. If I changed the trigger to 5 seconds and left the producer at 30 s, how often would you see a batch, and why?
2. A row arrives at silver with event_ts 19:41:00. The newest event_ts Spark has seen so far is 19:44:00. Watermark is 2 minutes. Does the row land or get dropped?

1. i would see a batch once since producer runs every 30s, so new offsets will come in after 30s when the sparks triggers it the 6th time
2. it gets added, window -> 19:40-19:45, latest 19:44, watermark is 19:42, so the window is not yet closed, an event of 19:41 comes, it lands in the window. window is open, not row is newer

 observations: Every one of the 20 rows in a batch has the same event_ts, and the skew to ingested_at is ~48 s in every batch: 19:40:01 → 19:40:49, 19:42:33 → 19:43:21, 19:43:36 → 19:44:24. Last week's median was 4.5 s.
will revisit this in silver compute

event_ts really is the prediction-generation time, which matters for your 48 s puzzle

two queries means two consumers, two reads of every Kafka message, two checkpoints

as of today 9/10/2026, So the topic now holds 1,604,979 messages, spark.read.parquet(bronze).count() must equal 1,604,977 and spark.read.text(quarantine).count() must equal 2

ingested_at is what we use to build the date/hour shelf labels from, it is easier to control. we'd use event_ts in silver as thats when we deal with watermarks
bronze answers "when did we receive it", silver answers "when did it happen"

highest file name in data\checkpoints\bronze_arrivals\commits is currently 35 
then i restarted the bronze arrival and it picked up from where it stopped at 35 and continued to 36 and when it got to 80 i.e 0 to 80, it stopped adding ( 1,604,979/20,000)
1,604,977 good rows in, 1,604,977 out

spaek checkPointLocation and _spark_metadata are responsible for the batches to resume at 36 and not get duplicated
checkpoints prevents gaps and spark meta data prevents duplicates

seems tfl keeps same ID for each prediction
event_ts tells you which photo a row came from, not which train. It's a version stamp, not an identity
understanding the aggregate function:
byId.count() ≈  good_df.groupby("id").size().reset_index(name="count")
counts.agg(min, avg, ...) ≈  counts["count"].describe()

train line_id of 000 means tfl don't know what train that is

An arrival: one train approaching one station, across many predictions over ~45 minutes. Identity: (line_id, vehicle_id, naptan_id)

chosen dedup keys: line_id, vehicle_id, naptan_id, event_ts
one prediction = one TfL generation for one train at one station

if the vehicle id is 000,000 rows are excluded from the delay metric and counted as unattributable, and that count is itself a data-quality number

ingested_at will not be in the dedup because ofcourse each ingested at would be different and will return a unique role count of 1 for each grouping. 

+----------+---+------------------+----+------+----+
|n_arrivals|min|avg               |p50 |p99   |max |
+----------+---+------------------+----+------+----+
|16928     |1  |117.00425330812854|18.0|1580.0|1829|
+----------+---+------------------+----+------+----+
Two honest explanations:

1. The train was cancelled, reversed short, or the tracking system lost it. Real-world noise.
2. Every train still on the board when you killed the producer has a "lowest countdown" of wherever it happened to be.

Anything whose lowest countdown is above 60 s is not completed: cancelled, lost, or truncated by session end

the threshold to get the delay would be 60s, any train whose arrival time is above 60s is not completed as the p50 gave a value of 18 for the last prediction of the trains arriving at a station. p99 = 1580 meant the train disnt get completed

delay = last expected_arrival − first expected_arrival

the watermark does two jobs with one number: it decides what is too late to accept, and therefore what is safe to forget.

plain dropDuplicates only bounds state if the event-time column is in the key; dropDuplicatesWithinWatermark bounds it regardless. My key happens to include event_ts, so both work, and I chose the one whose safety doesn't depend on that accident

state is the pile of things Spark must remember to finish a job (seen keys, open sums); the watermark is the one number that lets it empty the pile.

bronze for line-status and disruptions still to do; simple copies, no watermark needed
expected_arrival = event_ts + time_to_station

dropDuplicates is the batch truth
dropDuplicatesWithinWatermark is the streaming version that gets the same answer with bounded memory

ran batch dropDuplicates and got 1056017 
1,056,017. Ratio = 1,587,891 / 1,056,017 = 1.504


dropDuplicates becomes dropDuplicatesWithinWatermark when we are streaming and we add the withWatermark line before it

dedupe keeps an arbitrary platform row; completed share moves 86.6 → 86.4 %; accepted and documented, min/max are duplicate-proof anyway.

when i tried to get the delay info from the grouped train arrivals (after dedupe), found out that the data was actually for two days meaning that a train from sep 8 to a particular train station coud have its earliest and latest time wrong. A train is not delayed if it actually completes a trip and returns to the same station, i.e bank train arrives at west silvertown at 7:00am (first expected) then from the tfl data we still see that the train still arrives at 12:00pm and we think there is a 3 hr delay, but its not, it actually completed the ride and started a new one,

so to counter this, we use session_window("event_ts", "10 minutes") on the event_ts so we see a delay only after the 10 minutes window on the event_ts

if we use 10 minutes for session window, n= 39222, p50= 155, completed arrivals with delay = 43

if 5 minutes, n= 41450, p50= 147, completed arrivals with delay = 27
if 15 minutes, n= 37119, p50= 171, completed arrivals with delay = 58

now we need to consider if a train arrives earlier than tfl predicted and first expected now becomes the min bewtween expecred arrival and event_ts and vice cersa for last expected


the result is as follows

+-----+-----------+------------------+-----------+-----------+-----------+
|n    |min_delay_s|avg_delay_s       |p50_delay_s|p99_delay_s|max_delay_s|
+-----+-----------+------------------+-----------+-----------+-----------+
|28854|-1371      |147.28602620087335|41.0       |1932.0     |9939       |
+-----+-----------+------------------+-----------+-----------+-----------+

Number of completed arrivals with delay: 41

completed had 73.6% of the grouped_kept without duplicates

session_window splits by silence in the data, session_window answers "which trip is this row part of"
window splits by the clock, window answers "which reporting bucket does this finished trip belong to"

after grouping into windows, nothing was lost 28854 in and out

percentiles like p99 belong in the hourly or daily marts where n is in the hundreds.

first and last windows of every collection session are edge-truncated; exclude or flag them in the marts

a 0-second delay and "no data" are different facts, and the dashboard should show them differently

Refactored the silcer_arrivals.py into 6 functions and a if main, so that it can be imported into different loations e.g pytest

min_by/max_by survived the streaming switch

Append mode emits a session only after the watermark passes the session's end

Two stateful operators keep two state stores on disk, on Windows, on local[2]

decided to run two jobs for the window_reliability so it has its onw table is is debuggable when something breaks

after running the stream and writing to parquet:
Mentor said ~50 files, real number 774. The "handful of batches" prediction failed because I guessed at a number I could have counted.

801 Parquet silver files for what should be under 40k rows. Small-files problem, parked for Week 7

in the silver stage, : 39,222 − 34,179 = 5,043 arrivals missing, about 13%
completed 28,854 − 27,031 = 1,823 missing. Incomplete 10,368 − 7,148 = 3,220 missing
most of the missing rows are trains that were still approaching when collection stopped

Append mode plus a 2-minute watermark plus a 10-minute session gap means the last 12 minutes of every collection run never leave Spark's state. Measured: 5,043 of 39,222 arrivals (12.9%), of which 3,220 were incomplete trains still on the board. Silver's newest arrival is 19:03:56, bronze's newest event is 19:16:06.

My streaming job was 5,043 rows short of batch. I predicted a few hundred and was wrong. The gap was exactly watermark delay plus session gap, 12 minutes, and I proved it by measuring both timestamps to the second

performed the kill drill, restarted the silver arrivals, stopped midway and then continued later, it didn't create a duplicate, checked the silver data and all good

mentor predicted orphans, got none; kill probably landed before the write phase; the optional disk-vs-catalogue check from Day 2 has now been run once on silver and matched

1. Your streaming read needed the schema handed to it, but the batch read didn't. Why does streaming refuse to work it out itself?
2. withWatermark("event_ts", "2 minutes") changes zero rows. So what does it actually do, and why does the job need it at all? Two jobs, one number.
3. Batch gave 39,222 arrivals, streaming gave 34,179. Explain the 5,043 gap, including where the 12 minutes comes from, and say whether the missing rows are lost for good or just not written yet.
4. In the console run, Batches 0 to 3 were empty and Batch 4 had rows from 19:40. Why did the 19:40 sessions take four batches to appear?
5. You killed the silver job mid-drain and restarted. Name the two ledgers, say which failure each one prevents, and say which one only Spark reads.
6. You chose two jobs (D15). Give the reason you would give an interviewer, and one honest argument for the other choice.


1. streaming requires us to give it a schema because it doesn't peek into the data before handling it, just as a conyeyor belt, the packages are placed on it to the place they are boxed, unlike batch that has all the data already boxed and a schema that changes mid-run would corrupt the query, so Spark refuses to guess
2. it decides what is too late to accept, and therefore what is safe to forget, it changes no row but acts as an informer, in the sense that it just tells the dropDuplicateWithWatermark what column it has to watch as its watermark and the watermark duration
3. the 5043 gap was from trains still approaching when collection was stopped, its the watermark delay plus the session gap, not lost. Those 5,043 sessions are sitting in the checkpoint's state store
4. only after the watermark passes the session's end, goes past the first row's end 19:50, (19:52) does append occur to a batch, that's why it too that long
5. ledger one is the checkpoint (data/checkpoints/silver_arrivals): which bronze files are done. Prevents gaps and sets the restart point, which is why you resumed at 2, Ledger two is _spark_metadata inside data/silver/arrivals: which Parquet files officially belong to the table, written only after a batch's files are all on disk. Prevents duplicates from half-written files.
6. it is the purer streaming design, one checkpoint, one failure surface, lower latency for the windows because they don't wait for a second job, Debuggable, own table