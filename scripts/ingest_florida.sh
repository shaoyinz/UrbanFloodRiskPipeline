#!/usr/bin/env bash
# Land Florida raw data into gs://${PROJECT_ID}-floodpipe-raw/.
#
# Pulls (in order):
#   1. Overture buildings (FL bbox slice of pinned release)  ~1.5 GB
#   2. FEMA NFHL state geodatabase (S_FLD_HAZ_AR + S_BFE)    ~150 MB
#   3. USGS 3DEP 1/3" DEM tiles covering FL                  ~10-20 GB
#
# All steps are idempotent: re-running skips anything already in GCS.
# Pass --dry-run as the second arg to print the plan without touching
# either S3 or GCS.
#
# Usage:
#   PROJECT_ID=my-gcp-project scripts/ingest_florida.sh
#   PROJECT_ID=my-gcp-project scripts/ingest_florida.sh --dry-run

set -euo pipefail

: "${PROJECT_ID:?PROJECT_ID env var is required}"

EXTRA_ARGS=()
if [[ "${1-}" == "--dry-run" ]]; then
  EXTRA_ARGS+=(--dry-run)
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${REPO_ROOT}/.venv/bin/python"
export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

echo "== overture =="
"${PY}" -m floodpipe.ingest.overture --project-id "${PROJECT_ID}" "${EXTRA_ARGS[@]}"

echo "== fema nfhl =="
"${PY}" -m floodpipe.ingest.nfhl --project-id "${PROJECT_ID}" "${EXTRA_ARGS[@]}"

echo "== usgs 3dep =="
"${PY}" -m floodpipe.ingest.dem --project-id "${PROJECT_ID}" "${EXTRA_ARGS[@]}"

echo "all done"
