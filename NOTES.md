7/13/2026
CPU - Intel core i7 9th gen, 2.60ghz
RAM - 16gb
Free space - 125gb


Postgres couples storage and compute on one machine, so you scale them together whether you need to or not. Snowflake stores data once in cheap object storage and runs stateless compute clusters against it on demand, billed per second — so you scale compute independently, run isolated workloads on the same data, and pay nothing when idle

WBGCTLM-QP79037.snowflakecomputing.com
Activation date - 7/13/2026
standard edition
chose eu west 2 cus same region as I, keeps the storage-integration handshake simple and transfer costs at zero

creating the warehouse

XSMALL — the smallest compute cluster, 1 credit/hour when running. Everything in this project runs on X-Small; bigger sizes just burn trial credits faster for no benefit at your data volumes.
AUTO_SUSPEND = 60 — that's seconds of idle time before it switches itself off. This is the pitfall line: the default is 600, and a warehouse without it burns credits doing literally nothing.
AUTO_RESUME = TRUE — it wakes automatically the next time a query needs it. You never manually start/stop.
INITIALLY_SUSPENDED = TRUE — create it off, so the billing clock doesn't start until your first query.