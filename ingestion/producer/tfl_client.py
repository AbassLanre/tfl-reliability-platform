"""HTTP client for the TfL Unified API: timeouts, retries, backoff."""

import time
import logging
import requests

from ingestion.producer import config

log = logging.getLogger(__name__)

MAX_RETRIES = 4          # total attempts per request
TIMEOUT_S = 10           # never wait longer than this for a response
RETRYABLE = {429, 500, 502, 503, 504}

# One Session = one pooled connection, reused across requests.
# Cheaper and politer than a fresh TCP+TLS handshake every poll.
_session = requests.Session()



def _get(path: str, params: dict | None = None):
    """GET {base}{path}, return parsed JSON. Retries transient failures
    with exponential backoff; raises immediately on our-fault errors."""
    url = f"{config.TFL_BASE_URL}{path}"
    params = {**(params or {}), "app_key": config.TFL_APP_KEY}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session.get(url, params=params, timeout=TIMEOUT_S)
            if resp.status_code in RETRYABLE:
                # transient: fall through to backoff below
                log.warning("got %s from %s (attempt %d)", resp.status_code, path, attempt)
            else:
                resp.raise_for_status()   # 4xx = our fault -> raises, no retry
                return resp.json()        # 2xx -> done
        except requests.RequestException as exc:
            if isinstance(exc, requests.HTTPError):
                raise                      # the our-fault case from raise_for_status
            log.warning("network error on %s (attempt %d): %s", path, attempt, exc)

        if attempt == MAX_RETRIES:
            raise RuntimeError(f"giving up on {path} after {MAX_RETRIES} attempts")
        time.sleep(2 ** (attempt - 1))     # 1s, 2s, 4s


# --- public API
def get_arrivals(line_id):
  if line_id not in config.TUBE_LINES:
    raise ValueError(f"invalid line_id {line_id}, must be one of {config.TUBE_LINES}")
  return _get(f"/Line/{line_id}/Arrivals")

def get_line_statuses(modes:list[str]):
  if not all(mode in config.TFL_MODES for mode in modes):
    raise ValueError(f"invalid modes {modes}, must be one of {config.TFL_MODES}")
  return _get(f"/Line/Mode/{','.join(modes)}/Status")

def get_disruptions(line_id:str):
  if line_id not in config.TUBE_LINES:
    raise ValueError(f"invalid line_id {line_id}, must be one of {config.TUBE_LINES}")
  return _get(f"/Line/{line_id}/Disruption")