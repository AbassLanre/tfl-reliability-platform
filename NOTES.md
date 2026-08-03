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