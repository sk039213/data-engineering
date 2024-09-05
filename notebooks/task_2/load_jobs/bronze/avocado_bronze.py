# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

username = spark.sql("select regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"
file_path = "/Volumes/data_engineering/data_engineering/customer_data"
table_name = f"data_engineering.avocado_bronze"


df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("inferSchema", "true")
  .option("header", "true")
  .load(file_path)
  .select
        (
          col("consumerid").alias("consumer_id"),
          col("purchaseid").alias("purchase_id"),
          "avocado_bunch_id",
          "plu",
          col("ripe index when picked").alias("ripe_index_when_picked"),
          col("born_date").alias("born_at"),
          col("picked_date").alias("picked_at"),
          col("sold_date").alias("sold_at"),
          col("_metadata.file_path").alias("source_file"),
          current_timestamp().alias("processing_time")
        )
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))
