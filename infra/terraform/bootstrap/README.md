# Terraform state bootstrap

`bootstrap.sh` creates one thing: the GCS bucket that holds Terraform's
remote state. Everything else (raw / silver / gold zone buckets) is
managed by Terraform in the parent directory.

## Why a bootstrap step exists

Terraform's `gcs` backend needs the state bucket to **already exist**
before `terraform init` will succeed. You cannot create the state bucket
inside the same Terraform configuration that uses it as a backend
without a dependency loop. The standard workaround is a one-time
imperative bootstrap.

The state bucket is intentionally **not imported** into Terraform
afterwards. Keeping it out of state means a careless `terraform destroy`
cannot delete the very bucket holding the state file.

## Usage

```bash
PROJECT_ID=your-gcp-project ./bootstrap.sh
```

Idempotent — re-runs are safe.

Optional env vars:

| Var         | Default                          | Notes                              |
|-------------|----------------------------------|------------------------------------|
| `PROJECT_ID`| (required)                       | GCP project that owns the bucket   |
| `PREFIX`    | `${PROJECT_ID}-floodpipe`        | Bucket name prefix                 |
| `LOCATION`  | `us-central1`                    | Single-region is fine for state    |

The script writes `infra/terraform/backend.hcl`, which is consumed by
`terraform init -backend-config=backend.hcl`.

## Tearing down

The state bucket is not in Terraform, so `terraform destroy` won't touch
it. To remove it manually:

```bash
gcloud storage rm --recursive gs://${PROJECT_ID}-floodpipe-tfstate
```

Do this **after** `terraform destroy` has cleared the data buckets,
otherwise you'll lose the state file describing what still exists.
