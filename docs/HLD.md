# High-Level Design — Cloud Data Warehouse (Redshift + Glue Orchestration)

## 1. Domain context

Olist is a Brazilian marketplace connecting small sellers to large e-commerce channels. The public dataset covers ~100K orders with items, prices, freight, payments (installments!), customer geolocation, and post-purchase review scores. Business questions the warehouse should answer:

- Revenue and order volume trends by month, product category, and state.
- Delivery performance: actual vs. estimated delivery date, by seller/region.
- Payment behavior: installment counts and payment type mix by order value band.
- Customer review score vs. delivery delay correlation.

These are the standard "sales performance + fulfillment + customer satisfaction" questions asked of any retail data warehouse.

## 2. Architecture overview

```
 S3 raw/olist/*.csv                Secrets Manager
        │                          (redshift creds)
        ▼                                │
┌───────────────┐                        │
│ Glue Job       │  CSV → Parquet         │
│ (schema        │  (processed/olist/)    │
│  normalize)    │                        │
└──────┬─────────┘                        │
       │ Parquet in S3                    │
       ▼                                  │
┌────────────────────┐   EventBridge rule │
│ EventBridge         │──(S3 PutObject)──▶│
│ (S3 upload trigger) │                    │
└──────────┬───────────┘                   │
           ▼                               │
┌─────────────────────────┐                │
│  Step Functions          │◀───────────────┘
│  state machine:          │
│  1. Run Glue Job          │
│  2. COPY → Redshift stage │
│  3. Run transform SQL     │
│  4. Notify (SNS)          │
└──────────┬───────────────┘
           ▼
┌─────────────────────────┐
│ Amazon Redshift Serverless│
│  staging.*  (raw-ish)     │
│  dw.fact_*, dw.dim_*      │
└───────────────────────────┘
```

## 3. Components

| Component | Purpose |
|---|---|
| S3 `raw/olist/` | Landing zone for the 9 Olist CSVs |
| Glue Job | Type-normalizes and converts CSV → Parquet in `processed/olist/`, partitioned where it makes sense (e.g., orders by order_purchase year/month) |
| EventBridge rule | Fires on new object in `processed/olist/orders/` prefix, starts the Step Functions execution |
| Step Functions | Orchestrates: Glue job (if not already run) → Redshift `COPY` into staging → SQL transform into star schema → success/failure notification |
| Redshift Serverless | Compute for staging load + dimensional transform + BI queries |
| Secrets Manager | Stores Redshift admin/app credentials; Step Functions and Glue read them at runtime, never hardcoded |
| SNS | Success/failure notification from the state machine |

## 4. Target dimensional model

```
                dim_date
                    │
dim_customer──fact_orders──dim_seller
                    │
              fact_order_items
                    │
               dim_product
```

- **fact_orders** — grain: one row per order. Measures: order value, freight value, payment installments, delivery delay days.
- **fact_order_items** — grain: one row per order line item. Measures: price, freight value, quantity=1.
- **dim_customer**, **dim_seller** — SCD Type 1 (dataset has no historized attribute changes to track).
- **dim_product** — includes category name (translated to English via the dataset's category-translation file).
- **dim_date** — standard date dimension generated once, not loaded from source.

## 5. Data flow

1. Raw Olist CSVs uploaded to `raw/olist/`.
2. Glue job normalizes types (dates, decimals) and writes Parquet to `processed/olist/<entity>/`.
3. New object in the orders prefix triggers the EventBridge rule → starts Step Functions execution.
4. Step Functions: `COPY`s each processed Parquet prefix into its `staging.*` Redshift table (one table at a time, `TRUNCATE` then `COPY`, so re-runs are idempotent), then calls the `dw.sp_transform_orders()` stored procedure that upserts into `dw.dim_*` and rebuilds the affected `dw.fact_*` rows, then publishes an SNS success/failure message.
5. BI tools / Redshift Query Editor query the `dw` schema directly.

## 6. Key exam concepts exercised

- Redshift distribution styles (`KEY`/`EVEN`/`ALL`) and sort keys (`COMPOUND`/`INTERLEAVED`) chosen deliberately per table (see LLD).
- `COPY` command error handling and `IAM_ROLE` authentication (no embedded keys) — this build loads by S3 prefix rather than a manifest, since Glue's Spark output filenames aren't deterministic enough for a hand-authored manifest to stay valid; see LLD §4 for the trade-off and how to add manifest-based loading as an enhancement.
- Step Functions orchestration with `Catch`/`Retry` for failure handling and tracking.
- Secrets Manager integration for database credentials (via Redshift Serverless's managed admin password, not a hand-created secret).
- EventBridge event-driven triggering vs. a fixed schedule.

## 7. Non-functional considerations

- **Cost:** Redshift Serverless bills per RPU-second; workgroup configured with a modest base capacity and auto-pause is not applicable to Serverless (it scales to zero automatically when idle), which keeps this cheap for a personal project.
- **Idempotency:** Staging tables are truncated before each `COPY`; the transform stored procedure uses `MERGE`/delete-then-insert patterns into `dw.*` so re-running a failed execution doesn't duplicate facts.
- **Security:** Redshift encrypted with a customer-managed KMS key; `COPY` uses an IAM role, not access keys; the admin credential is AWS-managed in Secrets Manager (`manage_admin_password`), so no password ever appears in Terraform state or code.

## 8. Infrastructure as Code (Terraform)

[`infra/`](../infra/) provisions the S3 bucket, Redshift Serverless namespace/workgroup, the Glue CSV→Parquet job, the Step Functions state machine, and the EventBridge trigger — everything except the actual table/procedure DDL inside Redshift, which Terraform has no native resource for (see LLD §9 for why that's a deliberate boundary, not an oversight).

```
infra/
├── main.tf                     # all resources — see docs/LLD.md §9 for the inventory
├── variables.tf, outputs.tf, providers.tf, versions.tf
├── terraform.tfvars.example
└── modules/                      # vendored — kms_key, s3_data_lake_bucket, alerting
```

**Workflow:**

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform apply

# one-time schema bootstrap (Terraform can't run arbitrary SQL):
cd ..
./scripts/run_sql.sh "$(terraform -chdir=infra output -raw redshift_workgroup)" dev \
  "$(terraform -chdir=infra output -raw redshift_admin_secret_arn)"

# upload the 9 Olist CSVs to raw/olist/<entity>/ to kick off the pipeline
```

`terraform destroy` tears down the namespace/workgroup, state machine, Glue job, and bucket together — Redshift Serverless has no idle-pause billing quirk to worry about (it already scales to zero), so destroying between sessions isn't required for cost control the way it is for Project 4's EMR cluster, but is still the clean way to fully decommission the stack.
