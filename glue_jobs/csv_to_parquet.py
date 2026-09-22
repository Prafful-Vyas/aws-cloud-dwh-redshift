import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "processed_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW = args["raw_path"].rstrip("/")
PROCESSED = args["processed_path"].rstrip("/")


def read_csv(entity):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(f"{RAW}/{entity}/")


def write_parquet(df, entity):
    df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{PROCESSED}/{entity}/")


# Column names/selections below are chosen to line up 1:1 with sql/01_staging_ddl.sql
# so the Redshift COPY command (which matches Parquet fields to table columns by name)
# works without an extra mapping step.

orders = read_csv("orders").select(
    "order_id", "customer_id", "order_status",
    F.col("order_purchase_timestamp").cast("timestamp").alias("order_purchase_ts"),
    F.col("order_delivered_customer_date").cast("timestamp").alias("order_delivered_ts"),
    F.col("order_estimated_delivery_date").cast("timestamp").alias("order_estimated_ts"),
)
write_parquet(orders, "orders")

order_items = read_csv("order_items").select(
    "order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"
)
write_parquet(order_items, "order_items")

customers = read_csv("customers").select(
    "customer_id",
    F.col("customer_city").alias("customer_city"),
    F.col("customer_state").alias("customer_state"),
)
write_parquet(customers, "customers")

sellers = read_csv("sellers").select("seller_id", "seller_city", "seller_state")
write_parquet(sellers, "sellers")

category_translation = read_csv("category_translation")
products = (
    read_csv("products")
    .join(category_translation, on="product_category_name", how="left")
    .select(
        "product_id",
        F.coalesce("product_category_name_english", "product_category_name").alias("category_name_en"),
        "product_weight_g",
    )
)
write_parquet(products, "products")

payments = read_csv("payments").select("order_id", "payment_type", "payment_installments", "payment_value")
write_parquet(payments, "payments")

# Kept in the processed (Parquet) layer for direct Athena querying, but not
# loaded into Redshift staging — the star schema doesn't need them yet.
write_parquet(read_csv("reviews"), "reviews")
write_parquet(read_csv("geolocation"), "geolocation")

job.commit()
