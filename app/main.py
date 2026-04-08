import os
import requests
from waggle.plugin import Plugin
from datetime import datetime, timezone

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


def main():
    r = requests.get(URL, auth=(LOGGER_USER, LOGGER_PASS), timeout=20)
    r.raise_for_status()
    payload = r.json()

    fields = payload["head"]["fields"]
    row = payload["data"][0]
    timestamp_str = row["time"]
    timestamp = int(datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    values = row["vals"]

    with Plugin() as plugin:
        for field, value in zip(fields, values):

            if value == "NAN" or value is None:
                continue

            try:
                value = float(value)
            except Exception:
                continue

            measurement_name = field["name"].lower()
            unit = field.get("units", "")

            plugin.publish(
                name=measurement_name,
                value=value,
                timestamp=timestamp,
                meta={
                    "site": SITE,
                    "station_name": STATION_NAME,
                    "device": DEVICE,
                    "logger_ip": LOGGER_IP,
                    "table": TABLE_NAME,
                    "unit": unit,
                },
            )


if __name__ == "__main__":
    main()
