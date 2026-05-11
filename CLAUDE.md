# Urban Flood Risk Analysis Pipeline

## Project goal

Build a scalable, end-to-end data pipeline on Google Cloud Platform that
computes a defensible building-level flood vulnerability score for urban
infrastructure across the continental United States, using global building
footprints, FEMA flood hazard layers, and USGS digital elevation models.

The pipeline is designed to scale to ~2 billion global building footprints.
The portfolio implementation runs on CONUS (~130M buildings) to keep cost
bounded while exercising the same architectural patterns.

## Output

A BigQuery table with one row per building containing:

- Building identifier (Overture GERS ID), geometry, archetype
- Hazard attributes: FEMA flood zone, base flood elevation (BFE), depth at
  100-yr and 500-yr return periods, distance to nearest water, topographic
  position index
- Vulnerability output: damage fraction at each return period, **Expected
  Annual Damage (EAD)** in dollars, ML-predicted flood probability with
  per-building SHAP attributions for top drivers
- Equity overlay: Social Vulnerability Index (SVI) at the census tract,
  equity-weighted EAD
- H3 cell IDs at resolutions 6 through 9 for multi-scale aggregation

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Cloud Storage (raw zone)                                        │
│  ├── overture/release=YYYY-MM-DD/theme=buildings/    GeoParquet  │
│  ├── fema/nfhl/state=XX/                              GeoParquet │
│  ├── usgs/3dep/                                       COG tiles  │
│  └── ancillary/{nhd, nlcd, svi, ...}                             │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│  Cloud Composer (Airflow 2.x)  ── orchestration                  │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│  Dataproc Serverless (Spark 3.5 + Apache Sedona)                 │
│  - Reproject to EPSG:4326                                        │
│  - Repartition buildings by H3 res-7 cell                        │
│  - Spatial join: buildings ⨝ flood zones ⨝ DEM samples           │
│  - Feature engineering: TPI, slope, distance metrics             │
│  - Write enriched GeoParquet to silver zone                      │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│  BigQuery (gold zone)                                            │
│  - External tables over silver GeoParquet, then materialize      │
│  - dbt models compute archetypes, depth-damage, EAD, equity      │
│  - ML susceptibility model: BigQuery ML or Vertex AI             │
│  - Output: building_vulnerability fact table                     │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
       Looker Studio dashboard  +  Kepler.gl static export
```

### Why this shape

- **Spark + Sedona** handles raster-vector and large vector-vector joins that
  BigQuery is awkward at, especially DEM elevation sampling for hundreds of
  millions of building centroids.
- **BigQuery** handles the post-join scoring SQL where rows are independent.
  Cheaper, faster, and far more reproducible than running it in Spark.
- **Medallion separation** (raw → silver → gold) makes lineage explicit and
  lets us reprocess any stage without rerunning the others.
- **Airflow** orchestrates idempotent, parameterized-by-release runs. We
  pin the Overture release version per run, not "latest."

### Storage layer (decisions)

GCS layout is four buckets, all managed by Terraform except the
state bucket (chicken-and-egg). Naming: `${project_id}-floodpipe-{zone}`.

| Bucket    | Location          | Versioning | Lifecycle                  |
|-----------|-------------------|------------|----------------------------|
| `tfstate` | `us-central1`     | on         | none                       |
| `raw`     | `US` multi-region | off        | Nearline @30d, Coldline @90d |
| `silver`  | `US` multi-region | off        | Nearline @60d              |
| `gold`    | `US` multi-region | on         | none                       |

Defaults applied to every bucket: uniform bucket-level access (no ACLs),
public-access-prevention enforced, labels `project=floodpipe`,
`managed_by=terraform`, `zone=...` for cost attribution.

Rationale:

- **`US` multi-region for data**: BigQuery US multi-region reads from US
  multi-region GCS with zero egress. Single-region data + multi-region
  BigQuery would force a copy or pay per-query egress.
- **Versioning off on raw/silver**: many TB; both zones are reproducible
  (raw from pinned upstream releases, silver from raw via Spark rerun).
  Versioning would multiply storage cost without protecting anything not
  already recoverable.
- **Versioning on for gold/tfstate**: small, hot, expensive to recompute.
- **Lifecycle policies**: analytical reads cluster in the first 30–60d
  after ingest; cold tiers reduce long-tail cost ~5×.
- **State bucket bootstrapped, not Terraformed**: `terraform init`
  needs the bucket before it runs; also keeps `terraform destroy` from
  ever deleting the file describing what still exists.
- **Out of scope at this layer**: service accounts (added when Composer/
  Dataproc land), billing budget alerts (Cloud Console one-time per
  CLAUDE.md cost discipline note), Python GCS helpers (added when
  ingest jobs need them).

### BigQuery datasets

Two datasets are provisioned alongside the buckets, both in the `US`
multi-region:

| Dataset                | Purpose                                              |
|------------------------|------------------------------------------------------|
| `floodpipe_silver_ext` | External tables over silver-zone GeoParquet         |
| `floodpipe_gold`       | dbt fact tables (`fct_building_vulnerability`, H3)  |

Decisions:

- **Same `US` multi-region as the data buckets** so cross-reads are
  egress-free.
- **Two datasets, not one.** Silver-ext is read-only (truth lives in
  GCS); gold holds materialized dbt outputs. Splitting them keeps IAM
  grants tight (e.g. dbt service account writes to `gold` only).
- **`delete_contents_on_destroy = false`** on both — `terraform destroy`
  refuses to drop populated datasets. Manual drop required if you mean it.
- **BigQuery API enabled via Terraform** (`google_project_service` with
  `disable_on_destroy = false`) so a fresh project bootstraps cleanly
  without expanding `bootstrap.sh`'s API list.
- **No tables, views, or routines yet.** Schemas land with the dbt
  models in Phase 2; defining them in Terraform now would duplicate dbt's
  ownership of those objects.

See [README.md](./README.md) for operator commands.

---

## Methodology — probabilistic Expected Annual Damage

The vulnerability score is **Expected Annual Damage** in dollars, computed
per building. This replaces simple weighted-index approaches with the
formulation used in current flood-risk research and insurance practice:

```
Risk = Hazard (probability × intensity) × Exposure × Vulnerability
EAD  = ∫ damage(probability) d(probability)
```

### Component 1 — Multi-hazard decomposition

Three hazard types modeled separately because they have different physics
and different mitigation strategies:

- **Fluvial** (riverine overflow) — primary FEMA NFHL signal
- **Pluvial** (rainfall surface flooding from inadequate drainage) — *not*
  in NFHL; derived from topographic position index, drainage density, and
  impervious-surface fraction
- **Coastal** (surge + tide) — partially in NFHL VE/V zones

Each building gets three scores; the final EAD is the sum.

### Component 2 — Probabilistic depth grids

For each building, compute flood depth at multiple return periods
(10, 50, 100, 500 year). NFHL provides 100-yr and 500-yr footprints; we
interpolate intermediate return periods assuming a Generalized Extreme
Value (GEV) distribution on the depth-frequency curve.

```
depth_at_T = BFE_T - ground_elevation_at_centroid
```

Negative values are clipped to zero (building is above flood elevation).

**Phase-2 simplification.** With only two NFHL data points (T=100, T=500),
the full 3-parameter GEV is underdetermined. We use the Gumbel special
case (shape ξ=0), which has exactly two free parameters (location μ,
scale σ) and is uniquely solved from the (100, 500) pair. The reduced
variate is `y(T) = −ln(−ln(1 − 1/T))`; depth is linear in y. T<100
extrapolations are clamped at zero. See
`src/floodpipe/scoring/ead.py:interpolate_depth_gumbel`. Phase 4 swaps
this for a proper regional GEV fit using historic flood-frequency
analyses (e.g. NOAA Atlas 14 regional curves), where ξ varies by climate
zone.

### Component 3 — Depth-damage functions with archetypes

Damage is a **non-linear function of depth and building archetype**, not a
linear weighting. We use FEMA HAZUS depth-damage curves keyed by occupancy
class.

**Archetype assignment** from Overture attributes:

| Overture class/subtype          | HAZUS archetype |
|---------------------------------|-----------------|
| residential, single-family      | RES1            |
| residential, multi-family       | RES3            |
| commercial, retail              | COM1            |
| commercial, office              | COM4            |
| industrial                      | IND1            |
| education                       | EDU1            |
| medical                         | COM6            |
| (unmapped)                      | RES1 fallback   |

Number of stories from Overture `num_floors` refines the curve where
HAZUS distinguishes by stories.

**Damage fraction** at each return period:

```
damage_frac_T = DDF[archetype](depth_T)
damage_$_T    = damage_frac_T × replacement_value
```

Replacement value uses HAZUS default $/sqft tables × Overture footprint area
× number of floors. This is approximate but consistent and well-documented.

### Component 4 — EAD integration

Trapezoidal integration over the loss-frequency curve:

```
EAD = Σ (1/T_i − 1/T_{i+1}) × 0.5 × (D_i + D_{i+1})
```

where `T_i` are return periods (10, 50, 100, 500) and `D_i` is dollar damage
at that return period. The result is dollars per year of expected loss.

### Component 5 — ML susceptibility model

A binary classifier (XGBoost or LightGBM) predicts P(flood | building) using
all engineered features. Trained on historic flood occurrence labels from
the Global Flood Database (Dartmouth Flood Observatory + JRC GHSL) joined
to building footprints.

**Features** (all per building):

- Topographic: `elevation_m`, `slope_deg`, `tpi`, `flow_accumulation`
- Hydrologic: `dist_to_stream_m`, `dist_to_coast_m`, `drainage_density`
- Land cover: `impervious_pct` (NLCD), `curve_number`
- Hazard reference: `bfe_minus_elevation_m`, `fema_zone`
- Building: `archetype`, `footprint_area_m2`, `height_m`

**Validation:**

- Stratified spatial cross-validation by H3 res-6 cell (no nearby-building
  leakage between train and test)
- Target AUC ≥ 0.80
- Calibrated against FEMA NFHL zones at the building level (Spearman ρ)

**Explainability:** SHAP values per building. Surface the top three drivers
in the output table and dashboard. This is required, not optional — opaque
scores are unusable for downstream stakeholders.

### Component 6 — Equity overlay

Pull CDC Social Vulnerability Index at the 2022 census tract level. Per
building:

```
human_risk = EAD × SVI_tract_overall
```

Output both physical EAD and equity-weighted human_risk as separate columns.
Do not combine them into one number — that destroys the signal.

### Component 7 — H3 multi-resolution aggregation

Compute H3 cell IDs at resolutions 6, 7, 8, 9 per building. Aggregate
EAD and counts to each resolution for visualization. BigQuery's
`carto-os` UDFs provide H3 functions; falling back to the `h3` Python
library inside Spark also works.

---

## Data sources

All free and re-distributable.

### Building footprints — Overture Maps

- Path: `s3://overturemaps-us-west-2/release/{RELEASE}/theme=buildings/type=building/`
- Format: GeoParquet, partitioned, WKB geometry
- License: CDLA Permissive 2.0
- **Pin the release** in `config/release.yaml`. Overture retains only ~60
  days of public releases. Copy the slice we need to our GCS bucket on
  ingest, do not depend on the upstream long-term.

### FEMA flood hazards — NFHL

- Source: FEMA Map Service Center, state-level "NFHL_State_GDB" downloads
- Layers used: `S_Fld_Haz_Ar` (zones), `S_BFE` (base flood elevations)
- Format on download: file geodatabase. Convert to GeoParquet on ingest.
- Coverage: ~90% of US population. Document gaps in the README.

### Digital elevation — USGS 3DEP

- Path: `s3://prd-tnm/StagedProducts/Elevation/13/TIFF/` (1/3 arc-second, ~10m)
- Format: Cloud Optimized GeoTIFF
- Sample at building centroid; for larger buildings, also sample at vertices
  and use the minimum (most conservative for flood depth).

### Land cover — NLCD 2021

- Path: USGS NLCD 2021 product, Cloud Optimized GeoTIFF
- Used for impervious-surface fraction and curve number computation.

### Hydrography — USGS NHD

- High Resolution NHD, available as state-level downloads
- Used for distance-to-water features. Filter to flowlines and waterbodies
  with area ≥ 0.25 km² to match common flood-risk conventions.

### Historic flood labels — Global Flood Database

- Dartmouth Flood Observatory + JRC GHSL flood extents (2000–present)
- Used as ground truth for ML training labels.

### Social vulnerability — CDC SVI

- 2022 SVI at the tract level, downloaded as CSV
- Joined to buildings via tract intersection.

### NOT used (and why)

- First Street Flood Factor data — commercial license, not redistributable.
  Mention in docs as a benchmark target but do not ingest.
- Custom hydrodynamic model output — out of scope; we use NFHL plus
  GEV interpolation as a tractable approximation.

---

## Repo layout

```
.
├── CLAUDE.md                   # this file
├── README.md                   # public-facing
├── pyproject.toml              # uv project
├── .python-version             # 3.14
├── .pre-commit-config.yaml     # ruff, mypy, end-of-file fixer
├── config/
│   ├── release.yaml            # Overture release pin, regions
│   └── archetypes.yaml         # Overture → HAZUS mapping
├── dags/                       # Airflow DAGs (mirrored to Composer)
│   ├── flood_pipeline_dag.py
│   └── utils/
├── src/floodpipe/
│   ├── ingest/                 # Overture, NFHL, 3DEP downloaders
│   ├── spark/                  # Sedona jobs (spatial joins, sampling)
│   ├── features/               # TPI, slope, dist-to-water, etc.
│   ├── scoring/                # DDF lookup, EAD integration
│   ├── ml/                     # XGBoost training, SHAP
│   └── io/                     # GCS, BigQuery helpers
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/              # building_vulnerability fact, h3_aggregates
│   └── tests/
├── notebooks/
│   └── exploration/            # not in CI; experiments only
├── tests/
│   ├── unit/
│   └── integration/
└── infra/
    ├── terraform/              # GCP project, GCS, BQ, Composer
    │   ├── bootstrap/          # one-time tfstate bucket creation
    │   ├── versions.tf         # provider + backend pin
    │   ├── main.tf             # provider config
    │   ├── variables.tf
    │   ├── buckets.tf          # raw / silver / gold GCS buckets
    │   └── outputs.tf
    └── dbt_profiles.yml
```

---

## Coding conventions

- **Python 3.14**, managed with `uv`. Pin all versions. No
  `pyproject.toml` while the project is exploratory — packages on the
  PYTHONPATH via `tests/conftest.py` and `sys.path` in notebooks.
  Re-evaluate when Phase 2 Spark code needs to be packaged for Dataproc
  Serverless `--py-files`.
- **Linting**: `ruff` with rules in `pyproject.toml`. `mypy --strict` on
  `src/`, ignored in `notebooks/` and `tests/`.
- **Imports**: absolute from `floodpipe.*`. No `from x import *`.
- **Spark**: type-annotated PySpark with Sedona. Avoid `.toPandas()` on
  large DataFrames except for tiny lookups.
- **CRS**: everything reprojected to EPSG:4326 at ingest. Document any
  exception with a comment explaining why.
- **dbt**: snake_case, `stg_`, `int_`, `dim_`, `fct_` prefixes. Tests on
  every primary key (unique, not_null) and every foreign key.
- **Airflow**: DAGs idempotent and parameterized by `{{ ds }}` and the
  pinned Overture release. Use TaskFlow API. No global state outside DAG
  context.
- **Secrets**: GCP Secret Manager, accessed via Airflow connections. No
  keys in code, configs, or env-checked-in `.env`.
- **Logging**: `structlog` with JSON output. Include
  `run_id`, `release`, `region` in every log record.
- **Testing**: pytest. Spatial fixtures live in `tests/data/` as small
  GeoParquet files. Integration tests use Dataproc-emulator or local
  Sedona on tiny inputs.
- **Commits**: Conventional Commits (`feat:`, `fix:`, `chore:`, etc.).
  PRs squash-merged to `main`.
- **Notebook trust**: Jupyter signatures are content-based, so re-saving a
  notebook invalidates them and JupyterLab refuses to render maps/HTML
  output until you click "Trust Notebook". Run `make trust` after editing
  notebooks; it signs every `.ipynb` under `notebooks/` with the venv's
  `jupyter trust`.

---

## Phased build plan

### Phase 1 — Local prototype (≈ 1 week)
- One county (Harris County, TX or Miami-Dade, FL).
- DuckDB with the spatial extension end-to-end.
- Score and visualize on a folium map.
- **Goal:** prove the analytical chain before any GCP cost.

### Phase 2 — Single-region cloud pipeline (≈ 1–2 weeks)
- Lift Phase 1 to GCP, one state (Florida).
- Sedona on Dataproc Serverless.
- BigQuery with dbt models for scoring.
- Manual `gcloud` invocation; no Airflow yet.

### Phase 3 — Airflow orchestration (≈ 1 week)
- Wrap Phase 2 in a Cloud Composer DAG.
- Dynamic task mapping over states.
- Data-quality checks (Great Expectations or BQ assertions).
- Idempotency keyed on Overture release date.

### Phase 4 — ML and scale (≈ 2 weeks)
- Train XGBoost susceptibility model with SHAP.
- Add equity overlay (SVI join).
- Run for full CONUS.
- Optimize: H3 partitioning, broadcast joins, prune buildings by US bbox.

### Phase 5 — Polish (optional, ≈ 1 week)
- Looker Studio dashboard.
- CI/CD via Cloud Build (deploy DAGs and dbt on PR merge).
- Blog post / README writeup.

---

## Pitfalls and gotchas

- **Spatial-join skew**. A few dense urban tiles dominate. Without H3 or
  geohash partitioning, one Spark executor stalls the job. Always partition
  by H3 res-7 before joining.
- **CRS mismatches**. NFHL ships in state-plane projections that vary by
  state. Reproject on ingest. Add a unit test that asserts EPSG:4326 on the
  silver zone.
- **BigQuery GEOGRAPHY size limit** (~10 MB serialized). Some flood-zone
  multipolygons exceed this. Apply `ST_SIMPLIFY` with tolerance ≈ 1 m on
  ingest. Document the simplification in the dbt model.
- **DEM sampling cost**. Sampling 130 M centroids against COGs is the
  slowest step. Tile the DEM, broadcast small tiles per H3 partition, and
  sample with `RS_Value`. Cache the centroid→elevation lookup; reuse across
  Overture releases since building locations rarely move.
- **HAZUS DDF vintage**. The FEMA depth-damage curves are based on NFIP
  claims through ~2001. They are still standard but document this caveat
  in the README. Newer multi-variate (depth + duration) curves from
  HAZUS 7.0 can be added in a later iteration.
- **NFHL coverage gaps**. ~10% of the US population is not covered by
  effective NFHL. For those regions, fall back to ML-predicted susceptibility
  alone and flag the row with `hazard_source = 'ml_only'`.
- **Cost discipline**. Set GCP budget alerts at $5 / $10 / $20. Cap
  Dataproc Serverless executor counts. Use BigQuery on-demand billing,
  not flat-rate, until the workload is stable. Never run a CONUS-wide
  experiment without a cost estimate first.

---

## Out of scope (explicitly)

- **Global execution.** The pipeline is *designed* to scale to all 2B+
  Overture buildings, but the portfolio run is CONUS only. Document the
  scaling strategy in the README; do not actually pay for a global run.
- **Custom hydrodynamic modeling** (HEC-RAS, LISFLOOD-FP). Out of scope.
  We use NFHL plus GEV interpolation.
- **CMIP6 climate-adjusted depths.** Mention as a future-work extension.
  The First Street model does this; reproducing it is a project unto itself.
- **Real-time / streaming.** Batch-only. Daily orchestration is sufficient
  for this analytical use case.
- **Web frontend beyond Looker Studio / Kepler.** No bespoke React app.

---

## Decision log

When in doubt, prefer the option that:

1. Produces dollar-denominated outputs (EAD), not abstract scores.
2. Is reproducible from a pinned data release.
3. Generalizes to global scale even if not run globally.
4. Has SHAP-style explainability for any ML output.
5. Keeps the medallion zones independently reproducible.

If a request conflicts with these principles, surface the tension before
implementing.
