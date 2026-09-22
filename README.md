# Project 2 — Modern Cloud Data Warehouse: Redshift + Glue Orchestration

**Domain:** Retail / e-commerce order analytics
**Dataset:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — ~100K real orders (2016–2018) from a Brazilian marketplace, with customers, order items, products, sellers, payments, reviews, and geolocation across 9 relational CSV files. Free on Kaggle (CC BY-NC-SA 4.0 — fine for a personal/portfolio, non-commercial project).

## Why this domain

Retail/e-commerce is one of the largest employers of data engineers, and this is the canonical use case for a dimensional (star-schema) warehouse: orders and order-items as facts, customers/products/sellers/dates/geography as dimensions. The dataset already ships as normalized relational tables, which is exactly the shape you need to practice staging → dimensional modeling → distribution/sort key design.

## What this project builds

Raw CSVs land in S3 → Glue job converts to Parquet in the `processed/` tier → Redshift Serverless `COPY`s staging tables from S3 → SQL transform step builds a star schema (fact/dimension tables) → the whole sequence is orchestrated with Step Functions, triggered by EventBridge on new S3 uploads, with credentials pulled from Secrets Manager.

See [`docs/HLD.md`](docs/HLD.md) and [`docs/LLD.md`](docs/LLD.md) for full details, DDL, distribution/sort key rationale, and the Step Functions state machine definition.

## Deploying with Terraform

[`infra/`](infra/) provisions the bucket, Redshift Serverless namespace/workgroup, Glue job, Step Functions state machine, and the EventBridge trigger. See [`docs/LLD.md` §10](docs/LLD.md#10-terraform-resource-inventory) for the full resource inventory.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name
terraform init
terraform apply
```

Terraform has no resource for arbitrary SQL, so bootstrap the Redshift schema once, after `apply` (this creates the `staging`/`dw` schemas, all tables, and the `dw.sp_transform_orders()` procedure the pipeline calls):

```bash
cd ..
./scripts/run_sql.sh \
  "$(terraform -chdir=infra output -raw redshift_workgroup)" \
  "$(terraform -chdir=infra output -raw redshift_database)" \
  "$(terraform -chdir=infra output -raw redshift_admin_secret_arn)"
```

Then download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and upload its 9 CSVs to `raw/olist/<entity>/` — the pipeline runs automatically once `orders/` lands.

**Tear down:**

```bash
cd infra
terraform destroy
```

## Folder layout

```
02-cloud-dwh-redshift/               # this folder is its own git repo
├── .gitignore
├── README.md
├── docs/
│   ├── HLD.md
│   └── LLD.md
├── sql/
│   ├── 01_staging_ddl.sql
│   ├── 02_dimensional_ddl.sql
│   └── 03_transform.sql
├── scripts/
│   └── run_sql.sh
├── glue_jobs/
│   └── csv_to_parquet.py
├── stepfunctions/
│   └── state_machine.asl.json.tpl
└── infra/                          # Terraform: S3, Redshift Serverless, Glue, Step Functions, EventBridge
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── terraform.tfvars.example
    └── modules/                      # vendored — kms_key, s3_data_lake_bucket, alerting
```
