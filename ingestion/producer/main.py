"""Producer entrypoint: poll TfL on schedule, produce to Kafka."""

import json
import logging
import time

from confluent_kafka import Producer

from ingestion.producer import config, tfl_client, transform, dedupe

log = logging.getLogger("producer")
status_deduper = dedupe.Deduper()
disrupter_deduper = dedupe.Deduper()

def on_delivery(err, msg):
    if err:
        log.error("delivery failed to %s: %s", msg.topic(), err)


def poll_arrivals(p: Producer) -> None:
    sent = 0
    for line in config.TUBE_LINES:
        try:
            for raw in tfl_client.get_arrivals(line):
                m = transform.build_arrival(raw)
                p.produce("tfl.arrivals", key=m["line_id"],
                          value=json.dumps(m), callback=on_delivery)
                sent += 1
        except Exception:
            log.exception("arrivals poll failed for line %s", line)  # contain: next line
    log.info("arrivals: produced %d", sent)

def poll_line_status(p: Producer) -> None:
    sent = 0
    skipped =0
    try:
        for raw in tfl_client.get_line_statuses(["tube"]):
            for m in transform.build_line_status(raw):
              if status_deduper.is_new(m):
                p.produce("tfl.line-status", key=m["line_id"],
                          value=json.dumps(m), callback=on_delivery)
                sent += 1
              else:
                skipped +=1
    except Exception:
        log.exception("line status poll failed")
    log.info("line status: produced %d", sent)
    log.info("line status: skipped %d", skipped)

    
def poll_disruptions(p: Producer) -> None:
    sent = 0
    skipped =0
    for line in config.TUBE_LINES:
        try:
            for raw in tfl_client.get_disruptions(line):
                m = transform.build_disruptions(raw, line)
                if disrupter_deduper.is_new(m):
                    p.produce("tfl.disruptions", key=m["line_id"],
                              value=json.dumps(m), callback=on_delivery)
                    sent += 1
                else:
                  skipped +=1
        except Exception:
            log.exception("disruptions poll failed for line %s", line)  # contain: next line
    log.info("disruptions: produced %d", sent)
    log.info("disruptions: skipped %d", skipped)

def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    due = {"arrivals": 0.0, "status": 0.0, "disruptions": 0.0}

    try:
        while True:
            now = time.monotonic()
            if now >= due["arrivals"]:
                poll_arrivals(p)
                due["arrivals"] = now + config.ARRIVALS_POLL_S
            if now >= due["status"]:
                poll_line_status(p)
                due["status"] = now + config.STATUS_POLL_S
            if now >= due["disruptions"]:
                poll_disruptions(p)
                due["disruptions"] = now + config.DISRUPTION_POLL_S
            p.poll(1)  # service delivery callbacks; doubles as the loop's sleep
    except KeyboardInterrupt:
        log.info("interrupted - shutting down")
    finally:
        log.info("flushing outstanding messages...")
        p.flush(10)


if __name__ == "__main__":
    main()