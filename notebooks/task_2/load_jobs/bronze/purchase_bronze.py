# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"
file_path = "dbfs:/FileStore/tables/bronze/purchase" 
ddl_path = "ddl/bronze_dataset.sql"
table_name = "data_engineering.bronze.purchase"
username = spark.sql("select regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]

df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("inferSchema", "true")
  .option("header", "true")
  .load(file_path)
  .select
        (
          col("purchaseid").alias("purchase_id"),
          col("consumerid").alias("consumer_id"),
          "graphed_date",
          "avocado_bunch_id", 
          "reporting_year",
          col("QA process").alias("qa_process"),
          "billing_provider_sku",
          "grocery_store_id",
          "price_index",
          col("_metadata.file_path").alias("source_file"),
          current_timestamp().alias("processing_time")
        )
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))
