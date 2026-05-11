"""Phase 2 ingest scripts.

Each submodule is a CLI: pull one upstream source for Florida and land
it under a release-tagged path in the GCS `raw` zone. Modules expose
pure helpers (URL builders, bbox math, tile enumeration) that the unit
tests cover; the side-effecting glue lives in ``main()``.
"""
