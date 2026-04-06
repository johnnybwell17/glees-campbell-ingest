import json
import os
from pathlib import Path
from datetime import datetime, UTC
from typing import Any

import requests
from requests.auth import HTTPBasicAuth
import yaml


def load_config() -> dict:
    with open("config/logger_config.yaml", "r") as f:
        return yaml.safe_load(f)


def normalize_value(value: Any) -> Any:
    """Convert Campbell-style missing values into Python nulls."""
    if isinstance(value, str):
        if value.strip().upper() in {"NAN", "INF", "-INF", ""}:
            return None
    return value


def build_clean_record(payload: dict, site: str, logger_ip: str) -> dict:
    """Flatten Campbell JSON into one labeled, cleaned record."""
    env = payload["head"]["environment"]
    fields = payload["head"]["fields"]
    record = payload["data"][0]

    field_names = [f["name"] for f in fields]
    field_units = {f["name"]: f.get("units") for f in fields}
    field_process = {f["name"]: f.get("process") for f in fields}

    cleaned_values = {
        name: normalize_value(val)
        for name, val in zip(field_names, record["vals"])
    }

    clean_record = {
        "site": site,
        "logger_ip": logger_ip,
        "station_name": env.get("station_name"),
        "table_name": env.get("table_name"),
        "model": env.get("model"),
        "serial_no": env.get("serial_no"),
        "os_version": env.get("os_version"),
        "program_name": env.get("prog_name"),
        "record_time": record.get("time"),
        "record_number": record.get("no"),
        "fields": cleaned_values,
        "field_units": field_units,
        "field_process": field_process,
        "ingested_at_utc": datetime.now(UTC).isoformat(),
    }

    return clean_record


def build_labeled_points(clean_record: dict) -> list[dict]:
    """
    Convert one cleaned record into labeled point records.
    One output per measurement field.
    """
    points = []

    for field_name, value in clean_record["fields"].items():
        point = {
            "measurement": "campbell_logger",
            "site": clean_record["site"],
            "station_name": clean_record["station_name"],
            "logger_ip": clean_record["logger_ip"],
            "table_name": clean_record["table_name"],
            "field_name": field_name,
            "field_unit": clean_record["field_units"].get(field_name),
            "field_process": clean_record["field_process"].get(field_name),
            "timestamp": clean_record["record_time"],
            "value": value,
            "quality_flag": "missing" if value is None else "ok",
        }
        points.append(point)

    return points


def fetch_latest_payload(cfg: dict) -> dict:
    logger_ip = cfg["logger"]["ip"]
    logger_user = cfg["logger"]["user"]
    logger_table = cfg["logger"]["table"]
    logger_pass = os.getenv("LOGGER_PASS")

    if not logger_pass:
        raise SystemExit("LOGGER_PASS environment variable is required")

    url = (
        f"http://{logger_ip}/"
        f"?command=dataquery"
        f"&uri=dl:{logger_table}"
        f"&mode=most-recent"
        f"&p1=1"
        f"&format=json"
    )

    response = requests.get(
        url,
        auth=HTTPBasicAuth(logger_user, logger_pass),
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def save_json(obj: Any, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(obj, f, indent=2)


def main():
    cfg = load_config()

    site = cfg.get("site", {}).get("name", "glees")
    logger_ip = cfg["logger"]["ip"]

    payload = fetch_latest_payload(cfg)

    # Save raw
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    raw_path = Path("data/raw") / f"raw_{ts}.json"
    save_json(payload, raw_path)

    # Build cleaned record
    clean_record = build_clean_record(payload, site=site, logger_ip=logger_ip)
    clean_path = Path("data/clean") / f"clean_{ts}.json"
    save_json(clean_record, clean_path)

    # Build labeled points
    labeled_points = build_labeled_points(clean_record)
    labeled_path = Path("data/labeled") / f"points_{ts}.json"
    save_json(labeled_points, labeled_path)

    print(f"Saved raw:     {raw_path}")
    print(f"Saved clean:   {clean_path}")
    print(f"Saved labeled: {labeled_path}")
    print("\nExample labeled point:")
    print(json.dumps(labeled_points[0], indent=2))


if __name__ == "__main__":
    main()
