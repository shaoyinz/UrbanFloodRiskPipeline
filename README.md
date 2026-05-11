# Urban Flood Risk Analysis Pipeline

End-to-end GCP data pipeline that scores building-level flood risk in
**Expected Annual Damage (EAD) dollars** across the continental US,
combining Overture building footprints, FEMA NFHL flood hazards, and
USGS 3DEP elevation.

See [CLAUDE.md](./CLAUDE.md) for full architecture, methodology, data
sources, and the phased build plan.

---

## Status

- **Phase 0 — infra bootstrap.** ✓ Storage layer (4 GCS buckets) +
  BigQuery datasets in Terraform.
- **Phase 1 — local DuckDB prototype.** ✓ Harris County, TX end-to-end
  in `notebooks/exploration/01..05`. Final per-building EAD parquet at
  `data/raw/harris_building_ead.parquet`. Total Harris EAD ≈ $598 M/yr.
- **Phase 2 — single-region cloud pipeline.** in progress.
  - ✓ Dataproc & dbt service accounts + IAM (`infra/terraform/compute.tf`).
  - ✓ Pinned upstream releases in `config/release.yaml`; HAZUS archetypes
    and DDF tables extracted to `config/archetypes.yaml`.
  - ✓ Scoring library `src/floodpipe/scoring/` with 4-point Gumbel-tail
    EAD; verified to reproduce notebook-5 Harris totals exactly (24
    unit tests).
  - next: Florida ingest scripts (Overture + NFHL) → raw bucket;
    Sedona/Dataproc Serverless job → silver GeoParquet; dbt models →
    gold `fct_building_vulnerability`.

---

## Prerequisites

| Tool        | Version    | Notes                                                  |
|-------------|------------|--------------------------------------------------------|
| `gcloud`    | latest     | Authenticated against the target GCP project           |
| `terraform` | >= 1.7     | Uses the `gcs` remote backend                          |
| `uv`        | latest     | Python toolchain for Phase 1+; not needed for storage  |

The acting principal needs at minimum `roles/storage.admin` on the
target project, and `roles/serviceusage.serviceUsageAdmin` to enable
APIs (or `roles/owner` covers both).

---

## Set up GCP storage

The storage layer is **four GCS buckets** managed by Terraform.
Naming convention: `${project_id}-floodpipe-{zone}` (overridable via
`bucket_prefix`).

| Bucket               | Purpose                                       | Location          | Versioning | Lifecycle                  |
|----------------------|-----------------------------------------------|-------------------|------------|----------------------------|
| `<prefix>-tfstate`   | Terraform remote state                        | `us-central1`     | on         | none                       |
| `<prefix>-raw`       | Medallion raw zone (immutable upstream copies)| `US` multi-region | off        | Nearline @30d, Coldline @90d |
| `<prefix>-silver`    | Medallion silver (enriched GeoParquet)        | `US` multi-region | off        | Nearline @60d              |
| `<prefix>-gold`      | Medallion gold (BQ external tables, exports)  | `US` multi-region | on         | none                       |

Plus two BigQuery datasets in the same `US` multi-region (zero egress to
the data buckets):

| Dataset                       | Purpose                                                      |
|-------------------------------|--------------------------------------------------------------|
| `floodpipe_silver_ext`        | External tables over silver-zone GeoParquet (read-only)      |
| `floodpipe_gold`              | dbt-materialized fact tables and H3 aggregates               |

Both datasets have `delete_contents_on_destroy = false` — `terraform destroy`
will refuse to remove them while tables exist. Drop tables manually first
if you really mean it.

### Quickstart (Makefile)

```bash
make setup PROJECT_ID=your-gcp-project-id   # bootstrap + tfvars + init + plan
make apply                                   # review plan, then create buckets
```

`make setup` is safe — it stops at `terraform plan` so you can review
before any resource is created. `make apply` is the only step that
actually provisions anything.

To tear down: `make destroy` (state bucket stays — see "Why these
defaults" below).

### What the targets do (manual equivalent)

If you'd rather run each step yourself:

```bash
# 1. Bootstrap — creates only the tfstate bucket and writes backend.hcl.
cd infra/terraform/bootstrap
PROJECT_ID=your-gcp-project-id ./bootstrap.sh

# 2. Configure variables.
cd ..
cp terraform.tfvars.example terraform.tfvars   # then edit project_id

# 3. Initialize and apply.
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

### Tear down

```bash
make destroy
# or:  cd infra/terraform && terraform destroy
#
# State bucket is intentionally NOT in Terraform; remove manually if desired:
#   gcloud storage rm --recursive gs://<prefix>-tfstate
```

---

## Why these defaults

- **`US` multi-region for data buckets.** BigQuery US multi-region
  datasets read from US multi-region GCS buckets with **zero egress
  charge**. Single-region data with multi-region BigQuery would force
  either a copy step or per-query egress fees.
- **Uniform bucket-level access + public-access-prevention.** ACLs are
  a footgun and bypass IAM auditing. Enforced public-access prevention
  removes any path to accidental public exposure.
- **Versioning off on `raw`/`silver`.** Both zones grow to many TB.
  Object versioning would multiply storage cost. Both are
  reproducible — `raw` from the pinned upstream Overture/FEMA/USGS
  releases, `silver` from `raw` via a Spark rerun.
- **Versioning on for `gold` and `tfstate`.** Small footprints, hot
  reads, and expensive to recompute or recover. Cheap insurance.
- **Lifecycle to Nearline/Coldline.** Analytical reads cluster within
  the first 30–60 days of ingest. Older slices are rarely re-read but
  worth keeping for reproducibility.
- **State bucket bootstrapped, not Terraformed.** Avoids the
  chicken-and-egg of needing the backend bucket to exist before
  `terraform init`. Also means a careless `terraform destroy` cannot
  delete the state file describing what still exists.

---

## Cost expectations

Empty buckets cost nothing. Once data lands, US multi-region GCS is
~$0.026/GB/mo Standard, ~$0.010/GB/mo Nearline, ~$0.004/GB/mo Coldline.

Per CLAUDE.md, set GCP **billing budget alerts at $5 / $10 / $20**
before kicking off any large ingest. Budgets are not in Terraform yet —
configure them in the Cloud Console once on first project setup.

---

## Repo layout (current)

```
infra/terraform/
├── bootstrap/
│   ├── bootstrap.sh        # creates tfstate bucket, writes backend.hcl
│   └── README.md
├── versions.tf             # provider + backend pin
├── main.tf                 # provider config
├── variables.tf            # project_id, region, data_location, prefix
├── buckets.tf              # raw / silver / gold buckets
├── bigquery.tf             # silver_ext + gold datasets
├── services.tf             # APIs: BigQuery
├── compute.tf              # APIs: Dataproc/Compute/IAM; SAs + bucket/dataset IAM
├── outputs.tf              # bucket URLs, dataset IDs, SA emails
└── terraform.tfvars.example

config/                     # release pins + HAZUS archetype tables
├── release.yaml
└── archetypes.yaml

src/floodpipe/              # pure-python lib (no pyproject.toml; see CLAUDE.md)
├── scoring/
│   ├── archetypes.py       # Overture -> HAZUS occupancy
│   ├── ddf.py              # depth-damage lookup, replacement value
│   └── ead.py              # Gumbel depth extrapolation + EAD trapezoid
└── ingest/
    ├── _common.py          # config loader, FL bbox, gcloud subprocess
    ├── overture.py         # DuckDB+httpfs -> Overture FL slice in raw
    ├── nfhl.py             # FEMA state GDB -> S_FLD_HAZ_AR + S_BFE
    └── dem.py              # 3DEP 1/3" tiles for FL bbox

scripts/ingest_florida.sh   # run all three ingest CLIs in order

tests/                      # 39 tests; run `.venv/bin/python -m pytest tests/`
├── conftest.py
└── unit/{test_scoring.py, test_ingest.py}
```

### Running the scoring lib locally

```bash
.venv/bin/python -m pytest tests/        # all tests
```

The `tests/conftest.py` adds `src/` to `sys.path`, so no install step is
needed. Notebooks import the same way (`sys.path.insert(0, "src")`).

---

## Phase 2 — ingest Florida raw data

Three CLIs land upstream data into `gs://${PROJECT_ID}-floodpipe-raw/`
under release-tagged paths. All are idempotent (skip targets already in
GCS) and support `--dry-run` to print the plan only.

```bash
# preview
PROJECT_ID=your-gcp-project scripts/ingest_florida.sh --dry-run

# run everything (Overture ~1.5 GB, NFHL ~150 MB, 3DEP ~10–20 GB)
PROJECT_ID=your-gcp-project scripts/ingest_florida.sh

# or run one at a time
PYTHONPATH=src .venv/bin/python -m floodpipe.ingest.overture --project-id $PROJECT_ID
PYTHONPATH=src .venv/bin/python -m floodpipe.ingest.nfhl     --project-id $PROJECT_ID
PYTHONPATH=src .venv/bin/python -m floodpipe.ingest.dem      --project-id $PROJECT_ID --max-tiles 4
```

Output layout under `gs://<prefix>-raw/`:

```
overture/release=<r>/state=FL/buildings.parquet
fema/nfhl/snapshot=<s>/state=FL/{zones,bfe}.parquet
usgs/3dep/product=13/state=FL/USGS_13_<tile>.tif
```

Auth: the CLIs shell out to `gcloud storage cp`, so any active `gcloud
auth` (user creds or impersonated SA) works. No extra Python deps for
GCS. Source S3 buckets (Overture, USGS 3DEP) are public; FEMA MSC is
public HTTPS. The release pins live in `config/release.yaml` — bump
them to force a clean re-ingest.
