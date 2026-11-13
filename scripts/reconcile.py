"""Reconciliation: did every Kafka message of one day land in Snowflake RAW?

Run from the repo root, venv active, Kafka up:
    python scripts/reconcile.py 2026-10-28
Exit code 0 = counts match, 1 = mismatch (so Airflow can fail on it later).
"""
import sys
from datetime import datetime, timedelta, timezone

import snowflake.connector
from confluent_kafka import Consumer, TopicPartition

TOPIC = "tfl.arrivals"
BOOTSTRAP = "localhost:9092"


def kafka_counts_per_hour(day):
    """{hour: message count} for the 24 UTC hours of `day`. Reads page numbers only, no messages."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "reconcile"})
    partitions = list(c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)  # 0..10
    fence_posts = [day + timedelta(hours=h) for h in range(25)]                   # 25 posts = 24 hours

    def offsets_at(ts):
        """For each partition: the page number of the first message at or after time ts."""
        ms = int(ts.timestamp() * 1000)                                           # Kafka wants milliseconds
        found = c.offsets_for_times([TopicPartition(TOPIC, p, ms) for p in partitions], timeout=10)
        out = {}
        for tp in found:
            if tp.offset < 0:                                                     # -1 = nothing that late: use the end
                _, end = c.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10)
                out[tp.partition] = end
            else:
                out[tp.partition] = tp.offset
        return out

    marks = [offsets_at(ts) for ts in fence_posts]
    c.close()
    # messages in hour i = (page number at the end of the hour) - (page number at the start), summed over partitions
    return {fence_posts[i]: sum(marks[i + 1][p] - marks[i][p] for p in partitions) for i in range(24)}


def snowflake_counts_per_hour(day):
    """{hour: row count} for the same 24 hours, by ingested_at (the 'we received it' clock)."""
    conn = snowflake.connector.connect(connection_name="tfl", warehouse="TFL_DEV_WH")
    sql = """
        SELECT DATE_TRUNC('hour', raw:ingested_at::timestamp_ntz) AS h, COUNT(*)
        FROM TFL_DEV.RAW.ARRIVALS
        WHERE raw:ingested_at::timestamp_ntz >= %s
          AND raw:ingested_at::timestamp_ntz <  %s
        GROUP BY 1
    """
    fmt = "%Y-%m-%d %H:%M:%S"
    rows = conn.cursor().execute(sql, (day.strftime(fmt), (day + timedelta(days=1)).strftime(fmt))).fetchall()
    conn.close()
    return {h.replace(tzinfo=timezone.utc): n for h, n in rows}                  # Snowflake returns no-zone times; label them UTC


if __name__ == "__main__":
    day = datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    kafka = kafka_counts_per_hour(day)
    snow = snowflake_counts_per_hour(day)

    print(f"{'hour (UTC)':<18}{'kafka':>10}{'snowflake':>12}{'diff':>8}")
    total_k = total_s = 0
    for hour in sorted(kafka):
        k, s = kafka[hour], snow.get(hour, 0)
        if k or s:                                                                # skip hours where both are empty
            print(f"{hour:%Y-%m-%d %H:%M}  {k:>10}{s:>12}{k - s:>8}")
        total_k, total_s = total_k + k, total_s + s
    print(f"{'TOTAL':<18}{total_k:>10}{total_s:>12}{total_k - total_s:>8}")

    ok = total_k == total_s
    print("OK: counts match" if ok else "MISMATCH")
    sys.exit(0 if ok else 1)