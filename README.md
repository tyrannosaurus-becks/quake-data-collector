# Quake Data Collector

This is the code for a lightweight Lambda function meant to be triggered every minute. It looks at the USGS seismic data feed and ingests each seismic event into S3 for further analytics. When an event is seen multiple times, it's simply overwritten with the latest version of the event.

This is intended to populate a data lake for use in ML pipelines.
