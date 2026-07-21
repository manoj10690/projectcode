# Metadata-Driven Autonomous Data Ingestion Framework
### Self-Service Data Platform on GCP — Build Guide

This repo implements the architecture from Chapters 3–5 of your report:
a **metadata store** (Cloud SQL) drives a **generic Beam/Dataflow engine**
that ingests any registered source into a **medallion BigQuery lakehouse**
(Bronze → Silver → Gold), gated by **deterministic DQ rules**, orchestrated
by **Cloud Workflows / Composer**, and self-served through a **Cloud Run UI**.

```
metadata-ingestion-platform/
├── terraform/          # Phase 1 — all GCP infra (IaC)
├── sql/                 # Phase 2 — metadata DB schema + sample data
├── engine/              # Phase 3 — the metadata-driven Beam/Dataflow engine
├── bigquery/            # Phase 4 — Silver/Gold DDL
├── orchestration/       # Phase 5 — Cloud Workflows + Composer DAG
├── ui/                  # Phase 6 — self-service Cloud Run UI
```

## Prerequisites
- `gcloud` CLI authenticated (`gcloud auth login && gcloud auth application-default login`)
- Terraform >= 1.5
- Python 3.11+, `pip`
- A GCP project with billing enabled — set it:
  ```bash
  export PROJECT_ID=<your-project-id>
  gcloud config set project $PROJECT_ID
  ```

---

## Phase 1 — Infrastructure (Terraform)

```bash
cd terraform
terraform init
terraform apply \
  -var="project_id=$PROJECT_ID" \
  -var="db_password=$(openssl rand -base64 24)" \
  -var="env=dev"
```

This provisions: enabled APIs, a KMS keyring/key (CMEK), the `bronze` and
`quarantine-dlq` GCS buckets, a Cloud SQL Postgres instance + database
(`ingest_metadata`), three BigQuery datasets (`bronze`/`silver`/`gold`),
three least-privilege service accounts (UI / Dataflow / orchestrator), a
Cloud Run placeholder service, and a Cloud Workflows + Eventarc trigger.

Note the outputs — you'll need `metadata_db_connection_name` and
`bronze_bucket` in later phases:
```bash
terraform output
```

**Note:** the Cloud Run service (`google_cloud_run_v2_service.ingest_ui`)
references an image (`gcr.io/$PROJECT_ID/ingest-ui:latest`) that doesn't
exist yet on first apply. Build it in Phase 6, then `terraform apply` again
(or `gcloud run deploy` directly) to pick up the real image.

---

## Phase 2 — Metadata Database

Get the Cloud SQL public IP:
```bash
gcloud sql instances describe dev-ingest-metadata --format="value(ipAddresses[0].ipAddress)"
```

Apply the schema, then load one working sample pipeline
(edit the `<PROJECT_ID>` placeholders in `sql/02_sample_metadata.sql` first):
```bash
psql "host=<IP> dbname=ingest_metadata user=ingest_app sslmode=require" -f sql/01_schema.sql
psql "host=<IP> dbname=ingest_metadata user=ingest_app sslmode=require" -f sql/02_sample_metadata.sql
```

This creates: `pipelines`, `schema_ledger`, `dq_rules`, `pipeline_runs`,
`dq_alerts` — and registers one demo pipeline, `customer_orders_feed`,
with 4 DQ rules.

---

## Phase 3 — The Ingestion Engine

Test locally first (DirectRunner, no Dataflow cost):
```bash
cd engine
pip install -r requirements.txt
python beam_pipeline.py \
  --pipeline_id 11111111-1111-1111-1111-111111111111 \
  --input sample_data/customer_orders_sample.jsonl \
  --dlq_path /tmp/dlq \
  --output_table $PROJECT_ID:silver.customer_orders \
  --db_host <CLOUD_SQL_IP> --db_name ingest_metadata \
  --db_user ingest_app --db_password <pwd> \
  --runner DirectRunner
```

Check the results: `silver.customer_orders` in BigQuery should have the
3 clean rows; `/tmp/dlq/quarantine/...` should have the negative-amount
and empty-order_id rows; `/tmp/dlq/malformed/...` should have the broken
JSON line; and a drift alert should appear in `dq_alerts` for the
`loyalty_tier` column that wasn't in the registered schema.

Build the Dataflow Flex Template for production use:
```bash
gcloud builds submit --tag gcr.io/$PROJECT_ID/ingest-engine:latest .
gcloud dataflow flex-template build gs://$PROJECT_ID-dev-df-staging/templates/ingest-engine.json \
  --image "gcr.io/$PROJECT_ID/ingest-engine:latest" \
  --sdk-language "PYTHON" \
  --metadata-file metadata.json   # see note below
```
> Create a minimal `metadata.json` describing the template parameters
> (`pipeline_id`, `input`, `dlq_path`, `output_table`, `db_host`, `db_name`,
> `db_user`, `db_password`) per the [Flex Template spec](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates).

---

## Phase 4 — BigQuery Medallion Layer

```bash
bq query --use_legacy_sql=false < bigquery/ddl_silver_gold.sql
```
Silver tables are otherwise auto-created by the engine
(`CREATE_IF_NEEDED`); this DDL is for pre-defining stricter typed tables
and the Gold `daily_order_summary` aggregate.

---

## Phase 5 — Orchestration

**Event-driven (Cloud Workflows + Eventarc)** — already deployed by
Terraform in Phase 1. Drop a file into
`gs://$PROJECT_ID-dev-bronze/incoming/11111111-1111-1111-1111-111111111111/orders.json`
and it auto-triggers a Dataflow run.

**Scheduled (Cloud Composer)** — for nightly batch + retries + backfill:
```bash
gcloud composer environments create ingest-composer-env \
  --location us-central1 --image-version composer-2-airflow-2
gcloud composer environments storage dags import \
  --environment ingest-composer-env --location us-central1 \
  --source orchestration/airflow_dag.py
```
Set Airflow Variables `project_id`, `metadata_db_host`,
`metadata_db_password`, and an `ingest_metadata_db` Postgres connection
in the Airflow UI before it runs.

---

## Phase 6 — Self-Service UI (Cloud Run)

```bash
cd ui
gcloud builds submit --tag gcr.io/$PROJECT_ID/ingest-ui:latest .
cd ../terraform
terraform apply -var="project_id=$PROJECT_ID" -var="db_password=<same as phase 1>"
terraform output ui_url
```
Open `ui_url` — you can now register new pipelines, define schemas and
required-field rules, and watch runs / DQ alerts, all without touching
Terraform or the Beam code again.

---

## What maps to which report chapter
| Report section | Repo artifact |
|---|---|
| Ch.3 Requirements & self-service UI | `ui/` |
| Ch.4.2 Metadata DB schema | `sql/01_schema.sql` |
| Ch.4.3 Medallion architecture | `bigquery/ddl_silver_gold.sql` |
| Ch.4.5 Identity / least-privilege guardrails | `terraform/main.tf` (service accounts + IAM) |
| Ch.5.1 GCP environment setup | `terraform/` |
| Ch.5 core ingestion engine, schema drift, DQ gate | `engine/beam_pipeline.py` |
| Ch.5.1.1 Orchestration | `orchestration/` |

## Suggested build order for your demo/viva
1. Terraform apply → show provisioned resources in GCP console.
2. Run `sql/01_schema.sql` + `02_sample_metadata.sql` → show tables in Cloud SQL Studio.
3. Run the engine locally against `sample_data/customer_orders_sample.jsonl` →
   show clean rows in BigQuery, bad rows in the DLQ bucket, and a drift alert row.
4. Deploy the UI → register a second pipeline live, to demonstrate "self-service."
5. Drop a file in the bronze bucket → show Cloud Workflows firing Dataflow automatically.
