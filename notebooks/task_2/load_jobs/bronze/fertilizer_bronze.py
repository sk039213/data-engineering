# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

username = spark.sql("select regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"
file_path = "/Volumes/data_engineering/data_engineering/customer_data"
table_name = "data_engineering.fertilizer_bronze"

df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("inferSchema", "true")
  .option("header", "true")
  .load(file_path)
  .select
        (
          col("fertilizerid").alias("fertilizer_id"),
          col("purchaseid").alias("purchase_id"),
          col("consumerid").alias("consumer_id"),
          "type",
          "mg", 
          "frequency",
          col("_metadata.file_path").alias("source_file"),
          current_timestamp().alias("processing_time")
        )
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))
