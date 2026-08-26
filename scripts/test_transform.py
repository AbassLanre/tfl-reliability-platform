import json
from ingestion.producer.transform import build_arrival, build_line_status, build_disruptions

with open("docs/sample_payloads/disruptions_tube.json", encoding="utf-8") as f:
    raw_events = json.load(f)

msg = build_disruptions(raw_events[0],'bakerloo')
print(json.dumps(msg, indent=2))