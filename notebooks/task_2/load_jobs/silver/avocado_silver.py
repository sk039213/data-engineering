# Databricks notebook source
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.transform_utils import validate_and_enforce_schema, deduplicate_data, log_error

bronze_table = "data_engineering.avocado_bronze"
silver_table = "data_engineering.avocado_silver"
error_logs_table = "data_engineering.avocado_error_logs_silver"


# Define the expected schema
schema = StructType([
    StructField("purchase_id", LongType(), False),
    StructField("consumer_id", LongType(), False),
    StructField("avocado_bunch_id", IntegerType(), True),
    StructField("plu", IntegerType(), True),
    StructField("ripe_index_when_picked", IntegerType(), True),
    StructField("born_at", DateType(), True),
    StructField("picked_at", DateType(), True),
    StructField("sold_at", DateType(), True),
    StructField("raw_file_name", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


# Read data from the Bronze layer
df = spark.read.format("delta").table(bronze_table)

# Add the current timestamp to the updated_at column
df = df.withColumn("updated_at", current_timestamp())

# Validate and enforce schema
df_validated = validate_and_enforce_schema(df, schema)

# Deduplicate data on primary key
df_deduped = deduplicate_data(df_validated, ["purchase_id"])

# Data quality checks
df_clean = df_deduped.filter(
    col("purchase_id").isNotNull() &
    col("consumer_id").isNotNull()
)

# Identify rows failing data quality checks
invalid_rows = df_validated.filter(
    col("purchase_id").isNull() |
    col("consumer_id").isNull()
).withColumn("error_reason", lit("Null values in required columns"))

# Ensure the schema matches before merging
df_clean = df_clean.select([col(field.name) for field in schema.fields])

# Check if the Silver table exists
if not spark.catalog.tableExists(silver_table):
    df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    # Merge into Silver table using Delta Lake's merge functionality
    spark = SparkSession.builder.getOrCreate()
    delta_table = DeltaTable.forName(spark, silver_table)
    (delta_table.alias("delta_table")
        .merge(
            df_clean.alias("clean_table"),
            "delta_table.purchase_id = clean_table.purchase_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"Data from {bronze_table} upserted to Silver table: {silver_table}")

# Write invalid rows to the error logs table
if not spark.catalog.tableExists(error_logs_table):
    invalid_rows.write.format("delta").mode("overwrite").saveAsTable(error_logs_table)
else:
    invalid_rows.write.format("delta").mode("append").saveAsTable(error_logs_table)

