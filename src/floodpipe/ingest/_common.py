"""Shared ingest helpers: config loading, FL bbox, GCS shell-outs.

We use ``gcloud storage`` via subprocess rather than google-cloud-storage
to keep the Python deps minimal and to match Phase 2's "manual gcloud
invocation" stance. The operator is already authenticated with gcloud
to even run Terraform, so this borrows that auth.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RELEASE_YAML = REPO_ROOT / "config" / "release.yaml"

# Florida bbox in EPSG:4326, buffered to catch Key West and the panhandle
# without slicing off coastal buildings. (W, S, E, N). Upper edge held at
# 31.0 exactly (Florida's true northern limit) so 1°-tile enumerations
# don't pick up an ocean-only row of 3DEP tiles.
FLORIDA_BBOX = (-87.65, 24.40, -79.96, 31.0)


@dataclass(frozen=True)
class ReleaseConfig:
    overture_release: str
    overture_s3_uri: str
    nfhl_snapshot: str
    nfhl_msc_base: str
    dep_product: str
    dep_s3_uri: str
    target_state_abbr: str
    target_state_fips: str


def load_release_config(path: Path = DEFAULT_RELEASE_YAML) -> ReleaseConfig:
    raw = yaml.safe_load(path.read_text())
    return ReleaseConfig(
        overture_release=str(raw["overture"]["release"]),
        overture_s3_uri=str(raw["overture"]["s3_uri"]),
        nfhl_snapshot=str(raw["fema_nfhl"]["snapshot"]),
        nfhl_msc_base=str(raw["fema_nfhl"]["msc_base"]).rstrip("/") + "/",
        dep_product=str(raw["usgs_3dep"]["product"]),
        dep_s3_uri=str(raw["usgs_3dep"]["s3_uri"]),
        target_state_abbr=str(raw["target"]["state_abbr"]),
        target_state_fips=str(raw["target"]["state_fips"]),
    )


def raw_bucket(project_id: str, bucket_prefix: str | None = None) -> str:
    prefix = bucket_prefix or f"{project_id}-floodpipe"
    return f"gs://{prefix}-raw"


def gcloud() -> str:
    path = shutil.which("gcloud")
    if not path:
        raise RuntimeError("gcloud CLI not found on PATH")
    return path


def gcs_object_exists(uri: str) -> bool:
    """Return True iff `gs://bucket/object` exists. Uses gcloud storage ls."""
    if not uri.startswith("gs://"):
        raise ValueError(f"expected gs:// URI, got {uri!r}")
    proc = subprocess.run(
        [gcloud(), "storage", "ls", uri],
        capture_output=True, text=True, check=False,
    )
    return proc.returncode == 0 and uri.rstrip("/") in proc.stdout


def gcs_upload(local: Path, gcs_uri: str, *, recursive: bool = False) -> None:
    """gcloud storage cp local -> gs:// path. Raises CalledProcessError on failure."""
    cmd = [gcloud(), "storage", "cp"]
    if recursive:
        cmd.append("--recursive")
    cmd += [str(local), gcs_uri]
    subprocess.run(cmd, check=True)


def http_user_agent() -> str:
    """Be a good citizen — identify ourselves on USGS / FEMA fetches."""
    return os.environ.get(
        "FLOODPIPE_USER_AGENT",
        "floodpipe-ingest/0.1 (+https://github.com/shaoyinz/UrbanFloodRisk)",
    )
