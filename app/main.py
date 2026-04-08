import os
import re
import sys
from datetime import datetime, timezone

import requests
from waggle.plugin import Plugin

LOGGER_IP = os.getenv("LOGGER_IP", "10.31.81.50")
TABLE_NAME = os.getenv("TABLE_NAME", "Sage_5min")
LOGGER_USER = os.getenv("LOGGER_USER", "admin")
LOGGER_PASS = os.getenv("LOGGER_PASS", "foo")

SITE = os.getenv("SITE", "glees")
STATION_NAME = os.getenv("STATION_NAME", "60650")
DEVICE = os.getenv("DEVICE", "cr1000x-60650")

URL = (
    f"http://{LOGGER_IP}/"
    f"?command=dataquery&uri=dl:{TABLE_NAME}"
    f"&mode=most-recent&p1=1&format=json"
)


def iso_to_ns(timestamp_str: str) -> int:
    dt = datetime.fromisoformat(timestamp_str)
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def normalize_metric_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    return name.strip("_")


def main():
    try:
        r = requests.get(URL, auth=(LOGGER_USER, LOGGER_PASS), timeout=20)
        r.raise_for_status()
        payload = r.json()
    except requests.RequestException as e:
        print(f"ERROR: failed to fetch logger data: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: invalid JSON from logger: {e}", file=sys.stderr)
        sys.exit(1)

    data_rows = payload.get("data", [])
    fields = payload.get("head", {}).get("fields", [])

    if not data_rows:
        print("ERROR: logger returned no data rows", file=sys.stderr)
        sys.exit(1)

    if not fields:
        print("ERROR: logger returned no field metadata", file=sys.stderr)
        sys.exit(1)

    row = data_rows[0]
    timestamp_str = row.get("time")
    values = row.get("vals", [])

    if timestamp_str is None:
        print("ERROR: logger row missing 'time'", file=sys.stderr)
        sys.exit(1)

    if not values:
        print("ERROR: logger row missing 'vals'", file=sys.stderr)
        sys.exit(1)

    timestamp_ns = iso_to_ns(timestamp_str)

    print(f"Fetched record at {timestamp_str} from {LOGGER_IP} table {TABLE_NAME}")

    published_count = 0

    with Plugin() as plugin:
        for field, value in zip(fields, values):
            if value == "NAN" or value is None:
                continue

            try:
                value = float(value)
            except Exception:
                continue

            measurement_name = normalize_metric_name(field["name"])
            unit = field.get("units", "")

            print(f"Publishing {measurement_name}={value} {unit}")

            plugin.publish(
                name=measurement_name,
                value=value,
                timestamp=timestamp_ns,
                meta={
                    "site": SITE,
                    "station_name": STATION_NAME,
                    "device": DEVICE,
                    "logger_ip": LOGGER_IP,
                    "table": TABLE_NAME,
                    "unit": unit,
                },
            )

            published_count += 1

    print(f"Published {published_count} measurements.")


if __name__ == "__main__":
    main()
