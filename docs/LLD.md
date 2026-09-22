# Low-Level Design — Cloud Data Warehouse (Redshift + Glue Orchestration)

## 1. S3 layout

```
raw/olist/orders/olist_orders_dataset.csv
raw/olist/order_items/olist_order_items_dataset.csv
raw/olist/customers/olist_customers_dataset.csv
raw/olist/products/olist_products_dataset.csv
raw/olist/sellers/olist_sellers_dataset.csv
raw/olist/payments/olist_order_payments_dataset.csv
raw/olist/reviews/olist_order_reviews_dataset.csv
raw/olist/geolocation/olist_geolocation_dataset.csv
raw/olist/category_translation/product_category_name_translation.csv

processed/olist/orders/part-00000-<uuid>.snappy.parquet
processed/olist/order_items/part-00000-<uuid>.snappy.parquet
processed/olist/customers/...
processed/olist/sellers/...
processed/olist/products/...
processed/olist/payments/...
processed/olist/reviews/...          -- kept in Parquet for Athena, not loaded to Redshift
processed/olist/geolocation/...      -- kept in Parquet for Athena, not loaded to Redshift
```

Each entity is a flat prefix (no year/month partitioning — Olist is a fixed 2016–2018 historical dump, not an incrementally-arriving dataset, so partitioning by date wouldn't prune anything useful). `glue_jobs/csv_to_parquet.py` `coalesce(1)`s each entity to a single Parquet file per run, which is why §4's `COPY` loads by prefix rather than an exact filename.

## 2. Redshift: staging DDL

```sql
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.orders (
    order_id            VARCHAR(32),
    customer_id         VARCHAR(32),
    order_status        VARCHAR(20),
    order_purchase_ts   TIMESTAMP,
    order_delivered_ts  TIMESTAMP,
    order_estimated_ts  TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (order_purchase_ts);

CREATE TABLE staging.order_items (
    order_id       VARCHAR(32),
    order_item_id  SMALLINT,
    product_id     VARCHAR(32),
    seller_id      VARCHAR(32),
    price          DECIMAL(10,2),
    freight_value  DECIMAL(10,2)
)
DISTSTYLE EVEN;
-- staging tables use EVEN/no sort key: they're truncate-and-load scratch space,
-- not queried directly, so load throughput matters more than scan performance.
```

## 3. Redshift: dimensional (star schema) DDL

```sql
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.dim_date (
    date_key        INT PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE,
    year            SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    day_of_week     SMALLINT,
    is_weekend      BOOLEAN
)
DISTSTYLE ALL;   -- small, replicated to every node for fast joins from any fact

CREATE TABLE dw.dim_customer (
    customer_key     BIGINT IDENTITY(1,1),
    customer_id      VARCHAR(32),
    customer_city    VARCHAR(64),
    customer_state   CHAR(2)
)
DISTSTYLE ALL;

CREATE TABLE dw.dim_seller (
    seller_key    BIGINT IDENTITY(1,1),
    seller_id     VARCHAR(32),
    seller_city   VARCHAR(64),
    seller_state  CHAR(2)
)
DISTSTYLE ALL;

CREATE TABLE dw.dim_product (
    product_key       BIGINT IDENTITY(1,1),
    product_id        VARCHAR(32),
    category_name_en  VARCHAR(64),
    product_weight_g  INT
)
DISTSTYLE ALL;

CREATE TABLE dw.fact_orders (
    order_key            BIGINT IDENTITY(1,1),
    order_id              VARCHAR(32),
    customer_key           BIGINT,
    order_date_key          INT,
    order_status            VARCHAR(20),
    delivery_delay_days      INT,        -- delivered - estimated
    total_order_value        DECIMAL(12,2),
    total_freight_value       DECIMAL(12,2),
    payment_installments       SMALLINT
)
DISTSTYLE KEY DISTKEY (customer_key)
SORTKEY COMPOUND (order_date_key, customer_key);

CREATE TABLE dw.fact_order_items (
    order_item_key     BIGINT IDENTITY(1,1),
    order_key           BIGINT,
    product_key           BIGINT,
    seller_key              BIGINT,
    price                     DECIMAL(10,2),
    freight_value              DECIMAL(10,2)
)
DISTSTYLE KEY DISTKEY (product_key)
SORTKEY COMPOUND (product_key, seller_key);
```

### Distribution / sort key rationale

| Table | Dist style | Why | Sort key | Why |
|---|---|---|---|---|
| `dim_date`, `dim_customer`, `dim_seller`, `dim_product` | `ALL` | Small (≤ a few hundred K rows), replicated to every slice so any join against them is a local join with zero network shuffle | — | not queried standalone at scale |
| `fact_orders` | `KEY(customer_key)` | Co-locates order rows with the customer dimension for customer-centric joins/aggregations, the most common query pattern | `COMPOUND(order_date_key, customer_key)` | Most BI queries filter by date range first — compound sort key lets Redshift skip whole blocks via zone maps |
| `fact_order_items` | `KEY(product_key)` | Co-locates line items with product dimension for category/product rollups | `COMPOUND(product_key, seller_key)` | Product-then-seller is the typical drill-down order for catalog/revenue analysis |

`INTERLEAVED` sort keys were considered for `fact_orders` (queries filter by date *or* customer *or* status unpredictably) but rejected here: interleaved sort keys cost more on `VACUUM REINDEX` and this dataset's query pattern is date-first, so `COMPOUND` is the better default — noted here as the trade-off to be able to explain on the exam.

## 4. COPY command

```sql
TRUNCATE staging.orders;
COPY staging.orders
FROM 's3://<bucket>/processed/olist/orders/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/<project_name>-redshift-copy-role'
FORMAT AS PARQUET;
-- repeated for order_items, customers, sellers, products, payments
```

**Manifest vs. prefix, and why this build uses a prefix:** a manifest (`MANIFEST` keyword + a JSON file listing exact object URLs) is the textbook-correct choice for exam purposes — it guarantees exactly the intended set of files loads, protecting against partially-written objects from a concurrent Glue run. It requires knowing the output filenames ahead of time, though, and Spark's `coalesce(1)`/`repartition()` writers generate a fresh UUID in the filename on every run, so a checked-in manifest would go stale the moment the Glue job runs again. Loading by **prefix** instead sidesteps that, at the cost of losing the manifest's read-consistency guarantee. A production version of this pipeline would close that gap with an extra Step Functions task (e.g., a small Lambda that lists the just-written objects via `s3:ListObjectsV2` and writes a manifest JSON on the fly) — noted here as the documented enhancement rather than implemented, to keep the reference Terraform reliably applyable.
- `IAM_ROLE` authentication throughout — no static credentials anywhere in the pipeline.
- Each `TRUNCATE` + `COPY` pair is what makes staging loads idempotent: re-running the same execution twice produces the same staging contents both times.

## 5. Transform SQL (staging → star schema, upsert pattern)

The full script lives in [`sql/03_transform.sql`](../sql/03_transform.sql), wrapped in a `dw.sp_transform_orders()` stored procedure so the state machine can invoke it as one short `CALL` statement instead of embedding this whole script as an inline JSON string (see §6). Abridged excerpt:

```sql
-- dim_customer upsert
MERGE INTO dw.dim_customer AS tgt
USING (SELECT DISTINCT customer_id, customer_city, customer_state FROM staging.customers) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET customer_city = src.customer_city, customer_state = src.customer_state
WHEN NOT MATCHED THEN INSERT (customer_id, customer_city, customer_state)
    VALUES (src.customer_id, src.customer_city, src.customer_state);

-- fact_orders insert (idempotent: delete-then-insert for the batch's order_ids)
DELETE FROM dw.fact_orders WHERE order_id IN (SELECT order_id FROM staging.orders);

INSERT INTO dw.fact_orders (order_id, customer_key, order_date_key, order_status,
                             delivery_delay_days, total_order_value, total_freight_value,
                             payment_installments)
SELECT o.order_id, c.customer_key,
       CAST(TO_CHAR(o.order_purchase_ts, 'YYYYMMDD') AS INT),
       o.order_status,
       DATEDIFF(day, o.order_estimated_ts, o.order_delivered_ts),
       SUM(oi.price + oi.freight_value),
       SUM(oi.freight_value),
       MAX(p.payment_installments)
FROM staging.orders o
JOIN dw.dim_customer c ON c.customer_id = o.customer_id
JOIN staging.order_items oi ON oi.order_id = o.order_id
JOIN staging.payments p ON p.order_id = o.order_id
GROUP BY o.order_id, c.customer_key, o.order_purchase_ts, o.order_status,
         o.order_estimated_ts, o.order_delivered_ts;
```

## 6. Step Functions state machine

The real, Terraform-deployed definition is [`stepfunctions/state_machine.asl.json.tpl`](../stepfunctions/state_machine.asl.json.tpl) — a template with `${...}` placeholders that `infra/main.tf` fills in via `templatefile()` (job name, workgroup, secret ARN, copy-role ARN, bucket name, SNS topic ARN) so no environment-specific values are hardcoded in the checked-in file. Shape (abridged):

```json
{
  "StartAt": "RunGlueJob",
  "States": {
    "RunGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_job_name}" },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "CopyToStaging"
    },
    "CopyToStaging": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement.sync",
      "Parameters": {
        "WorkgroupName": "${workgroup_name}",
        "Database": "${database_name}",
        "SecretArn": "${secret_arn}",
        "Sql": "TRUNCATE staging.orders; COPY staging.orders FROM 's3://${bucket_name}/processed/olist/orders/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET; ... (repeated per staging table)"
      },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "RunTransform"
    },
    "RunTransform": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement.sync",
      "Parameters": {
        "WorkgroupName": "${workgroup_name}",
        "Database": "${database_name}",
        "SecretArn": "${secret_arn}",
        "Sql": "CALL dw.sp_transform_orders();"
      },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "NotifySuccess"
    },
    "NotifySuccess": { "Type": "Task", "Resource": "arn:aws:states:::sns:publish", "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Olist DWH load succeeded" }, "End": true },
    "NotifyFailure": { "Type": "Task", "Resource": "arn:aws:states:::sns:publish", "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Olist DWH load FAILED" }, "End": true }
  }
}
```

- Uses the **Redshift Data API** (`redshiftdata:executeStatement.sync`) rather than a JDBC connection from within the state machine — no network/VPC plumbing needed from Step Functions, and it natively resolves the DB password from Secrets Manager via `SecretArn`.
- `.sync` suffix makes each Task poll until the Glue job / SQL statement actually completes, rather than firing-and-forgetting.
- `RunTransform` calling a stored procedure (§5) rather than inlining the multi-statement transform script keeps this JSON readable and avoids fragile quoting/escaping.
- EventBridge rule (`source: aws.s3`, `detail-type: Object Created`, prefix filter on `processed/olist/orders/`) targets this state machine's ARN to start it — this requires **EventBridge notifications enabled on the bucket itself** (`aws_s3_bucket_notification.eventbridge { eventbridge = true }`), not just the rule; easy to forget by hand, which is exactly the kind of one-time setup step Terraform is good at making unforgettable.

## 7. IAM

- `<project_name>-redshift-copy-role` — trust policy allows `redshift.amazonaws.com`; permissions: `s3:GetObject`/`s3:ListBucket` on `processed/*` only.
- `<project_name>-sfn-role` — `glue:StartJobRun`/`GetJobRun`/`GetJobRuns`; `redshift-data:ExecuteStatement`/`DescribeStatement`/`GetStatementResult`; `secretsmanager:GetSecretValue` scoped to the namespace's managed admin secret; `sns:Publish` scoped to the one topic.
- `<project_name>-eventbridge-sfn-role` — trust policy allows `events.amazonaws.com`; permission: `states:StartExecution` scoped to this one state machine ARN.
- Redshift Serverless namespace encrypted with a customer-managed KMS key; admin credentials only ever read from Secrets Manager (AWS-managed via `manage_admin_password`), never stored in state machine input or Terraform state.

## 8. Monitoring

- EventBridge rule on `aws.states` **Step Functions Execution Status Change** (`status IN [FAILED, TIMED_OUT, ABORTED]`) → SNS, as a belt-and-braces alarm alongside the state machine's own `NotifyFailure` branch (catches failures the state machine itself couldn't reach, e.g. it being throttled before starting).
- Redshift **query monitoring rule (WLM)**: abort any query estimated to scan more than a threshold of rows in the ad-hoc queue, protecting the shared workgroup from a runaway BI query — documented here as a follow-up enhancement (Redshift Serverless QMR isn't yet in the Terraform).
- CloudTrail captures `redshift-data:ExecuteStatement` and `secretsmanager:GetSecretValue` calls for audit.

## 9. Cost estimate

- Redshift Serverless: base 8 RPU, billed per-second only while queries run; scales to zero — a personal project running loads a few times a week costs a few dollars/month.
- Glue job: same DPU-hour math as Project 1, negligible for this dataset size (~50 MB total CSV).
- Step Functions: Standard workflow, ~4 state transitions per run — effectively free at this volume (first 4,000 transitions/month free).

## 10. Terraform resource inventory

[`infra/main.tf`](../infra/main.tf) provisions everything except table/procedure DDL (see §6/§9 in the HLD for why that boundary exists and how `scripts/run_sql.sh` fills the gap).

| Resource | Terraform address | Notes |
|---|---|---|
| CMK, bucket, alert topic | `module.kms`, `module.data_lake`, `module.alerts` | vendored under `infra/modules/`, identical to Project 1's copies |
| Redshift namespace | `aws_redshiftserverless_namespace.this` | `manage_admin_password = true` — AWS creates/rotates the admin secret; its ARN is a resource attribute (`admin_password_secret_arn`), never a value we set |
| Redshift workgroup | `aws_redshiftserverless_workgroup.this` | `publicly_accessible = false`, deployed into the **default VPC's** subnets for simplicity (see note below) |
| Redshift security group | `aws_security_group.redshift` | self-referencing ingress on 5439; Query Editor v2 in the console works regardless of `publicly_accessible` since it connects via the AWS backend, not a direct socket |
| Glue job (CSV→Parquet) | `aws_glue_job.csv_to_parquet` | script uploaded via `aws_s3_object.etl_script`, MD5-tracked so changes trigger re-upload |
| Step Functions state machine | `aws_sfn_state_machine.olist_dwh_load` | built from the `.tpl` file via `templatefile()` |
| EventBridge trigger | `aws_s3_bucket_notification.eventbridge` + `aws_cloudwatch_event_rule.new_orders_processed` + `aws_cloudwatch_event_target.start_state_machine` | three resources working together, per §6 |
| IAM roles | `aws_iam_role.glue_etl` / `.redshift_copy` / `.step_functions` / `.eventbridge_sfn` | one per service principal, least-privilege inline policies |

**Why the default VPC:** Redshift Serverless requires subnet IDs across multiple AZs; standing up a dedicated VPC/subnet layout is real Terraform work that's orthogonal to what this project is meant to teach (VPC networking is more a Solutions Architect exam topic than DEA-C01), so `data.aws_vpc.default`/`data.aws_subnets.default` are used deliberately to keep the stack focused. A hardened version would move this into a dedicated private-subnet VPC.

**Destroy order:** `terraform destroy` removes the namespace/workgroup along with everything else in one pass — there's no Redshift-specific teardown ordering to worry about, unlike EMR (Project 4) where a still-running cluster needs to actually terminate before its instance profile/roles can be cleanly removed.
