# Glees Campbell Ingest

This app pulls the most recent record from a Campbell Scientific CR1000X datalogger at the Glees site and publishes numeric measurements into the SAGE data system from a Thor Blade over a private LAN connection.

The current target table is `Sage_5min`. The app is intended as a lightweight edge ingestion layer for climate and soil sensor data prior to broader scaling and automation.
