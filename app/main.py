import os
import requests
from waggle.plugin import Plugin

LOGGER_IP = "10.31.81.50"
TABLE_NAME = "Sage_5min"
LOGGER_USER = "admin"
LOGGER_PASS = os.environ["LOGGER_PASS"]

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
    timestamp = row[0]

    with Plugin() as plugin:
        for i, field in enumerate(fields[1:], start=1):
            value = row[i]

            if value == "NAN" or value is None:
                continue

            try:
                value = float(value)
            except Exception:
                continue

            plugin.publish(
                name=f"campbell.{field['name']}",
                value=value,
                meta={
                    "site": "glees",
                    "station_name": "60650",
                    "logger_ip": LOGGER_IP,
                    "table": TABLE_NAME,
                    "unit": field.get("units", ""),
                    "timestamp": timestamp,
                },
            )

if __name__ == "__main__":
    main()
