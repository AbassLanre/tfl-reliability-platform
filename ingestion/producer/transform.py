"""Transform raw TfL API objects into schema.md v1 messages."""

from datetime import datetime, timezone


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_arrival(raw: dict) -> dict:
    """One raw prediction object -> one tfl.arrivals message."""
    return {
        # --- envelope ---
        "schema_version": 1,
        "event_type": "arrival",
        "ingested_at": _utc_now_iso(),
        "event_ts": raw["timestamp"],          # arrivals' event time, per schema.md
        # tfl.arrivals table. raw["..."] if required, raw.get("...") if optional.
        "line_id": raw["lineId"],
        "time_to_station": raw["timeToStation"],
        "id": raw["id"],
        "vehicle_id": raw["vehicleId"],
        "naptan_id": raw["naptanId"],
        "station_name": raw["stationName"],
        "line_name": raw["lineName"],
        "platform_name": raw["platformName"],
        "current_location": raw["currentLocation"],
        "towards": raw.get("towards"),
        "expected_arrival": raw["expectedArrival"],
        "time_to_live": raw["timeToLive"],
        "mode_name": raw["modeName"],
        "direction": raw.get("direction"),
        "destination_name": raw.get("destinationName"),
        "destination_naptan_id": raw.get("destinationNaptanId"),
    }
    
def build_line_status(raw:dict)-> list[dict]:
    now =  _utc_now_iso()
    return [{
         # --- envelope ---
                "schema_version": 1,
                "event_type": "line_status",
                "ingested_at": now,
                "event_ts": now,
                "line_id": raw["id"],
                "status_severity": s["statusSeverity"],
                "status_severity_description": s.get("statusSeverityDescription"),
                "valid_from": (
                    None
                    if not (s.get("validityPeriods") or [{}])[0]
                    else (s.get("validityPeriods") or [{}])[0].get("validFrom")
                ),
                "valid_to": (
                    None
                    if not (s.get("validityPeriods") or [{}])[0]
                    else (s.get("validityPeriods") or [{}])[0].get("validTo")
                ),
                "reason": s.get("reason")
        
            }       
            for s in raw["lineStatuses"]
            ]

def build_disruptions(raw:dict, line_id: str)-> dict:
    now = _utc_now_iso()
    return {
        "schema_version": 1,
        "event_type": "disruption",
        "ingested_at": now,
        "event_ts": raw.get("lastUpdate") or now,  
        "line_id": line_id,
        "category": raw['category'],
        "category_description": raw.get('categoryDescription'),
        "description": raw.get('description'),
        "affected_routes": raw.get('affectedRoutes'),
        "affected_stops": raw.get('affectedStops'),
        "closure_text": raw.get('closureText')
    }
    