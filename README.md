# Urban Flood Risk Analysis Pipeline

End-to-end GCP data pipeline that scores building-level flood risk in
**Expected Annual Damage (EAD) dollars** across the continental US,
combining Overture building footprints, FEMA NFHL flood hazards, and
USGS 3DEP elevation.

See [CLAUDE.md](./CLAUDE.md) for full architecture, methodology, data
sources, and the phased build plan.

---

## Status

**Phase 0 — infra bootstrap.** Storage layer (GCS buckets) + BigQuery
datasets. No compute (Dataproc / Composer) yet.

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
├── services.tf             # google_project_service for BigQuery API
├── outputs.tf              # bucket gs:// URLs + BQ dataset IDs
└── terraform.tfvars.example
```
