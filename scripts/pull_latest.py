import json
import os
from pathlib import Path
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth
import yaml


def load_config():
    with open("config/logger_config.yaml", "r") as f:
        return yaml.safe_load(f)


def main():
    cfg = load_config()
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
    payload = response.json()

    raw_dir = Path(cfg["output"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_file = raw_dir / f"{logger_table}_{ts}.json"

    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(json.dumps(payload, indent=2))
    print(f"\nSaved to {out_file}")


if __name__ == "__main__":
    main()
