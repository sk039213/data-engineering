# Databricks notebook source
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.transform_utils import validate_and_enforce_schema, deduplicate_data, log_error

# Define the expected schema
schema = StructType([
    StructField("consumerid", LongType(), nullable=False),
    StructField("sex", StringType(), nullable=True),
    StructField("ethnicity", StringType(), nullable=True),
    StructField("race", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("raw_file_name", StringType(), nullable=True),
    StructField("load_timestamp", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True)  # Add updated_at column
])


bronze_table = "data_engineering.consumer_bronze"
silver_table = "data_engineering.silver.consumer_silver"
error_logs_table = "data_engineering.purchase_error_logs_silver"


# Read data from the Bronze layer
df = spark.read.format("delta").table(bronze_table)

# Add the current timestamp to the updated_at column
df = df.withColumn("updated_at", current_timestamp())

# Validate and enforce schema
df_validated = validate_and_enforce_schema(df, schema)

# Deduplicate data on primary key
df_deduped = deduplicate_data(df_validated, ["consumerid"])

# Data quality checks
df_clean = df_deduped.filter(
    col("consumerid").isNotNull()
)

# Identify rows failing data quality checks
invalid_rows = df_validated.filter(
    col("consumer_id").isNull()
).withColumn("error_reason", lit("Null values in required columns"))

# Ensure the schema matches before merging
df_clean = df_clean.select([col(field.name) for field in schema.fields])

# Check if the Silver table exists
if not spark.catalog.tableExists(silver_table):
    df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    # Merge into Silver table using Delta Lake's merge functionality
    delta_table = DeltaTable.forName(spark, silver_table)
    delta_table.alias("t") \
        .merge(
            df_clean.alias("s"),
            "t.consumer_id = s.consumer_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

print(f"Data from {bronze_table} upserted to Silver table: {silver_table}")

# Write invalid rows to the error logs table
if not spark.catalog.tableExists(error_logs_table):
    invalid_rows.write.format("delta").mode("overwrite").saveAsTable(error_logs_table)
else:
    invalid_rows.write.format("delta").mode("append").saveAsTable(error_logs_table)
