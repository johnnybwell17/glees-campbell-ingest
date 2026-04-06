import csv
import json
import os
import time
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


def fetch_latest_payload(cfg: dict, logger_pass: str) -> dict:
    logger_ip = cfg["logger"]["ip"]
    logger_user = cfg["logger"]["user"]
    logger_table = cfg["logger"]["table"]

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


def build_clean_record(payload: dict, site: str, logger_ip: str) -> dict:
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


def save_json(obj: Any, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(obj, f, indent=2)


def append_points_to_csv(points: list[dict], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "measurement",
        "site",
        "station_name",
        "logger_ip",
        "table_name",
        "field_name",
        "field_unit",
        "field_process",
        "timestamp",
        "value",
        "quality_flag",
    ]

    file_exists = csv_path.exists()

    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(points)


def write_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a") as f:
        f.write(message + "\n")


def main():
    cfg = load_config()
    logger_pass = os.getenv("LOGGER_PASS")
    if not logger_pass:
        raise SystemExit("LOGGER_PASS environment variable is required")

    site = cfg.get("site", {}).get("name", "glees")
    logger_ip = cfg["logger"]["ip"]
    table = cfg["logger"]["table"]
    interval = int(cfg["polling"]["interval_seconds"])

    raw_dir = Path(cfg["output"].get("raw_dir", "data/raw"))
    clean_dir = Path(cfg["output"].get("clean_dir", "data/clean"))
    labeled_dir = Path(cfg["output"].get("labeled_dir", "data/labeled"))
    log_dir = Path(cfg["output"].get("log_dir", "data/logs"))

    csv_path = labeled_dir / "points_latest.csv"
    log_file = log_dir / "poller.log"

    print(f"Starting poller for table '{table}' at logger {logger_ip}")
    print(f"Polling every {interval} seconds. Press Ctrl+C to stop.")

    try:
        while True:
            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            try:
                payload = fetch_latest_payload(cfg, logger_pass)

                raw_path = raw_dir / f"raw_{ts}.json"
                save_json(payload, raw_path)

                clean_record = build_clean_record(payload, site=site, logger_ip=logger_ip)
                clean_path = clean_dir / f"clean_{ts}.json"
                save_json(clean_record, clean_path)

                labeled_points = build_labeled_points(clean_record)
                labeled_path = labeled_dir / f"points_{ts}.json"
                save_json(labeled_points, labeled_path)

                append_points_to_csv(labeled_points, csv_path)

                msg = (
                    f"{ts} success "
                    f"raw={raw_path} "
                    f"clean={clean_path} "
                    f"labeled={labeled_path} "
                    f"csv={csv_path}"
                )
                write_log(log_file, msg)
                print(msg)

            except Exception as e:
                msg = f"{ts} error {e}"
                write_log(log_file, msg)
                print(msg)

            time.sleep(interval)

    except KeyboardInterrupt:
        stop_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        msg = f"{stop_ts} stopped_by_user"
        write_log(log_file, msg)
        print("\nPoller stopped by user.")


if __name__ == "__main__":
    main()
