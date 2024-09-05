# Databricks notebook source
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.transform_utils import validate_and_enforce_schema, deduplicate_data, log_error

# Define the expected schema
schema = StructType([
    StructField("purchase_id", LongType(), nullable=False),
    StructField("consumer_id", LongType(), nullable=False),
    StructField("graphed_date", DateType(), nullable=False),
    StructField("avocado_bunch_id", IntegerType(), nullable=True),
    StructField("reporting_year", IntegerType(), nullable=True),
    StructField("qa_process", StringType(), nullable=True),
    StructField("billing_provider_sku", IntegerType(), nullable=True),
    StructField("grocery_store_id", IntegerType(), nullable=True),
    StructField("price_index", IntegerType(), nullable=True),
    StructField("raw_file_name", StringType(), nullable=True),
    StructField("load_timestamp", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True)
])

bronze_table = "data_engineering.purchase_bronze"
silver_table = "data_engineering.purchase_silver"
error_logs_table = "data_engineering.purchase_error_logs_silver"

# Read data from the Bronze layer
df = spark.read.format("delta").table(bronze_table)

# Add the current timestamp to the updated_at column
df = df.withColumn("updated_at", current_timestamp())

# Validate and enforce schema
df_validated = validate_and_enforce_schema(df, schema)

# Deduplicate data on primary key
df_deduped = deduplicate_data(df_validated, ["purchase_id"])

# Identify rows failing data quality checks
invalid_rows = df_validated.filter(
    col("consumer_id").isNull()
).withColumn("error_reason", lit("Null values in required columns"))

# Filter valid rows
df_clean = df_deduped.filter(
    col("purchase_id").isNotNull() &
    col("consumer_id").isNotNull()
)

# Ensure the schema matches before merging
df_clean = df_clean.select([col(field.name) for field in schema.fields])

# Check if the Silver table exists and if it is a Delta table
if not DeltaTable.isDeltaTable(spark, silver_table):
    # If the table does not exist or is not a Delta table, create an empty Delta table
    df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    delta_table = DeltaTable.forPath(spark, silver_table)
    delta_table.alias("t") \
        .merge(
            df_clean.alias("s"),
            "t.purchase_id = s.purchase_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Write invalid rows to the error logs table
if not spark.catalog.tableExists(error_logs_table):
    invalid_rows.write.format("delta").mode("overwrite").saveAsTable(error_logs_table)
else:
    invalid_rows.write.format("delta").mode("append").saveAsTable(error_logs_table)

