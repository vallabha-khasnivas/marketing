import sys
from datetime import datetime, timedelta

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    explode,
    concat_ws,
    lit,
)

# =========================================================
# Job Arguments
# =========================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "table",
        "parentProject",
        "connectionName",
        "databucket",
        "gluetablename",
        "property_id",
        "timedelta_days",
        "property_name"
    ],
)
property_name=args["property_name"]
property_id = args["property_id"]
table = args["table"]
databucket = args["databucket"]
gluetablename = args["gluetablename"]
timedelta_days = int(args["timedelta_days"])

# =========================================================
# Spark / Glue Init
# =========================================================

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# =========================================================
# Resolve GA4 Daily Table
# =========================================================

target_date = datetime.today() - timedelta(days=timedelta_days)

table_suffix = target_date.strftime("%Y%m%d")

table_full = f"{table}_{table_suffix}"

logger.info(f"Reading table: {table_full}")

# =========================================================
# Read BigQuery GA4 Export
# =========================================================

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
        "viewsEnabled": "true",
        "table": table_full.strip(),
        "parentProject": args["parentProject"],
        "connectionName": args["connectionName"],
    },
    transformation_ctx="datasource",
)

# =========================================================
# Convert To Spark DataFrame
# =========================================================

df = datasource.toDF()

# =========================================================
# Add Metadata Columns
# =========================================================

df = df.withColumn(
    "property_id",
    lit(property_id),
)

df = df.withColumn(
    "property_name",
    lit(property_name),
)
df = df.withColumn(
    "join_key",
    concat_ws(
        "_",
        col("event_name"),
        col("event_timestamp"),
        col("user_pseudo_id"),
    ),
)

# =========================================================
# Explode event_params
# =========================================================

flattened_df = (
    df
    .select(
        "*",
        explode("event_params").alias("param_key", "param_value")
    )
    .withColumn("key", col("param_key"))
    .withColumn("string_value", col("param_value.string_value"))
    .withColumn("int_value", col("param_value.int_value"))
    .withColumn("float_value", col("param_value.float_value"))
    .withColumn("double_value", col("param_value.double_value"))
    .drop("param_key", "param_value")
)
# =========================================================
# Output Path
# =========================================================

output_path = (
    f"s3://ad-dl-prod-rawzone/marketing-projects/"
    f"{databucket}/"
    f"{gluetablename}/"
)

logger.info(f"Writing to: {output_path}")

# =========================================================
# Write Parquet
# =========================================================
flattened_df = flattened_df.repartition(
    "property_id",
    "event_date"
)
(
    flattened_df
    .write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .option("compression", "snappy")
    .partitionBy("property_id", "event_date")
    .parquet(output_path)
)

logger.info("GA4 flattening completed successfully.")

job.commit()