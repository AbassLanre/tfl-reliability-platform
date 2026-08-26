"""All producer configuration. Values come from environment variables;
a local .env file supplies them during development (never committed)."""

import os
from dotenv import load_dotenv

load_dotenv()  # reads .env from the current directory into os.environ, if present

# --- required: fail fast if missing ---
TFL_APP_KEY = os.getenv("TFL_APP_KEY")
if not TFL_APP_KEY:
    raise RuntimeError("TFL_APP_KEY is not set, add it to .env or the environment")

# --- optional: sensible defaults ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ARRIVALS_POLL_S = int(os.getenv("ARRIVALS_POLL_S", "30"))

STATUS_POLL_S = int(os.getenv("STATUS_POLL_S",'60'))
DISRUPTION_POLL_S = int(os.getenv("DISRUPTION_POLL_S","60"))
TFL_BASE_URL = os.getenv("TFL_BASE_URL","https://api.tfl.gov.uk")

# --- constants (not configurable - the tube map doesn't change via env var) ---
TUBE_LINES = ["bakerloo", "central", "circle", "district", "hammersmith-city", "jubilee", "metropolitan", "northern", "piccadilly", "victoria", "waterloo-city" ]
TFL_MODES = [
  "bus",
  "tube",
  "cable-car",
  "coach",
  "cycle",
  "cycle-hire",
  "dlr",
  "elizabeth-line",
  "goods",
  "interchange-keep-sitting",
  "interchange-secure",
  "national-rail",
  "overground",
  "replacement-bus",
  "river-bus",
  "river-tour",
  "taxi",
  "tflrail",
  "tram",
  "walking"
]