# Glees Campbell Ingest

A simple edge app for pulling the most recent data record from a Campbell Scientific CR1000X datalogger and publishing those measurements to Sage.

## What it does

This app runs on a Sage / Thor Blade and:

- connects to a Campbell CR1000X over HTTP
- reads the latest row from a logger table
- skips invalid `NAN` values
- publishes each numeric field as a Sage measurement
- attaches useful metadata such as site, device, logger IP, table name, and unit

## Current target

- **Site:** Glees
- **Logger:** Campbell Scientific CR1000X
- **Default logger IP:** `10.31.81.50`
- **Default table:** `Sage_5min`

## Configuration

These values can be set with environment variables:

- `LOGGER_IP`
- `TABLE_NAME`
- `LOGGER_USER`
- `LOGGER_PASS`
- `SITE`
- `STATION_NAME`
- `DEVICE`

Default values in the app:

- `LOGGER_IP=10.31.81.50`
- `TABLE_NAME=Sage_5min`
- `LOGGER_USER=admin`
- `SITE=glees`
- `STATION_NAME=60650`
- `DEVICE=cr1000x-60650`

## Example published metadata

Each measurement includes metadata like:

- `site`
- `station_name`
- `device`
- `logger_ip`
- `table`
- `unit`

The measurement timestamp is published as its own timestamp field.

## Main files

- `app/main.py` - main app logic
- `Dockerfile` - container build
- `requirements.txt` - Python dependencies
- `sage.yaml` - app metadata

## Status

This repo is being adapted from a local Campbell ingest pipeline into a reusable Sage edge app.
