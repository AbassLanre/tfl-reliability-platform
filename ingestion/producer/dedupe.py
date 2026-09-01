"""Content-based dedupe: skip messages identical to ones already sent."""

import hashlib
import json

VOLATILE = ("ingested_at", "event_ts")


def fingerprint(msg: dict) -> str:
    stable = {k: v for k, v in msg.items() if k not in VOLATILE}
    return hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()


class Deduper:
    def __init__(self, max_size: int = 100_000):
        self._seen: set[str] = set()
        self._max = max_size

    def is_new(self, msg: dict) -> bool:
        fp = fingerprint(msg)
        if fp in self._seen:
            return False
        if len(self._seen) >= self._max:
            self._seen.clear()   # crude but bounded; documented trade-off
        self._seen.add(fp)
        return True