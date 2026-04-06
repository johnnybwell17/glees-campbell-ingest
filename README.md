# Glees Campbell Ingest

## Overview

This project implements a local data ingestion and preprocessing pipeline for a Campbell Scientific CR1000X datalogger deployed at the Glees site, running on a Thor Blade within the SAGE (Sustaining AI for Global Ecosystems) system.

The pipeline retrieves environmental measurements from the datalogger over HTTP, performs lightweight cleaning and normalization, and produces structured outputs suitable for downstream ingestion, analysis, or integration with SAGE services.

---

## System Architecture
CR1000X (Campbell datalogger)
↓ (HTTP + basic auth)
Thor Blade (local Python pipeline)
↓
Raw / Clean / Labeled outputs
↓
(Optional) SAGE / cloud ingestion


### Network Configuration

- Logger IP: `10.31.81.50`
- Blade LAN: `10.31.81.1`
- Communication: HTTP (port 80)
- Authentication: Basic auth

The CR1000X is connected to the Thor Blade via a local switch on the Blade’s LAN interface, forming a private sensor network.

---

## Data Source

Data are retrieved from the CR1000X using its built-in HTTP API:

```bash
http://10.31.81.50/?command=dataquery&uri=dl:Sage_5min&mode=most-recent&p1=1&format=json

Table: Sage_5min

Example variables:

Batt_Volt_Min (battery voltage)
PTemp_C (panel temperature)
Precip_mm_Tot (precipitation)
VWC_5cm_Avg (soil volumetric water content)
EC_5cm_Avg (soil electrical conductivity)
T_5cm_Avg (soil temperature)


Project Structure
glees-campbell-ingest/
├── config/
│   └── logger_config.yaml
├── scripts/
│   ├── pull_latest.py
│   ├── clean_and_label.py
│   └── poll_logger.py
├── data/
│   ├── raw/
│   ├── clean/
│   ├── labeled/
│   └── logs/
├── docs/
│   └── network_notes.md
├── requirements.txt
└── README.md

Data Processing Pipeline

Each poll cycle performs the following steps:

1. Data Retrieval
Query CR1000X via HTTP API
Authenticate using environment variable credentials
2. Raw Storage
Save full unmodified JSON response
Location: data/raw/
3. Cleaning
Convert "NAN" → null
Normalize values
Attach metadata:
site
logger IP
station name
table name
timestamp

Output: data/clean/

4. Labeling
Convert one record → multiple labeled data points
One point per variable

Each point includes:

measurement name
site
logger metadata
field name
units
process type
timestamp
value
quality flag (ok or missing)

Output: data/labeled/

5. CSV Aggregation
Append labeled points to: 
data/labeled/points_latest.csv

Example Labeled Output
{
  "measurement": "campbell_logger",
  "site": "glees",
  "station_name": "60650",
  "logger_ip": "10.31.81.50",
  "table_name": "Sage_5min",
  "field_name": "Batt_Volt_Min",
  "field_unit": "Volts",
  "field_process": "Min",
  "timestamp": "2026-04-03T12:35:00",
  "value": 12.17,
  "quality_flag": "ok"
}
Setup
1. Create virtual environment
python3 -m venv venv
source venv/bin/activate
2. Install dependencies
pip install -r requirements.txt
Configuration

Edit:

config/logger_config.yaml
site:
  name: glees

logger:
  ip: 10.31.81.50
  user: admin
  table: Sage_5min

polling:
  interval_seconds: 300

output:
  raw_dir: data/raw
  clean_dir: data/clean
  labeled_dir: data/labeled
  log_dir: data/logs
Authentication

Set the logger password via environment variable:

export LOGGER_PASS='YOUR_PASSWORD'
Usage
Pull a single record
python3 scripts/pull_latest.py
Run continuous poller
python3 scripts/poll_logger.py

Stop with:

Ctrl + C
Logging

Logs are written to:

data/logs/poller.log

Includes:

success events
file outputs
error messages
Data Handling Philosophy

This pipeline follows a layered data model:

Layer	Purpose
Raw	Preserve original logger output
Clean	Normalize and structure data
Labeled	Standardize for downstream systems

This ensures:

reproducibility
auditability
flexibility for future processing
Future Work
Duplicate record detection (skip identical timestamps)
Integration with SAGE data pipelines
Automated deployment via systemd
Real-time streaming to cloud endpoints
Sensor QA/QC improvements
Multi-logger support
Notes
Missing values from the logger appear as "NAN" and are converted to null
Some soil variables may remain missing depending on sensor availability
This pipeline performs lightweight edge processing only; heavy aggregation and modeling should occur downstream
Summary

This project establishes a complete edge ingestion pipeline for Campbell dataloggers within the SAGE ecosystem, transforming raw sensor output into structured, labeled data suitable for scalable environmental monitoring and analysis.
