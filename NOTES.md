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

