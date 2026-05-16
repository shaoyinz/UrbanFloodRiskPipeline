"""Sedona-on-Dataproc-Serverless jobs that build the medallion silver zone.

The silver job (``build_silver``) lifts notebooks 03–04 to Florida scale:
it spatially joins Overture building footprints to FEMA NFHL flood zones,
samples the 3DEP DEM, and writes enriched GeoParquet for the dbt gold
step to read as a BigQuery external table.
"""
