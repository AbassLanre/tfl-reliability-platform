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
