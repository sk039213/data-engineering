# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"
file_path = "dbfs:/FileStore/tables/bronze/consumer"
ddl_path = "ddl/bronze_dataset.sql"
table_name = "data_engineering.bronze.consumer"
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
          col("consumerid").alias("consumer_id"),
          col("Sex").alias("sex"),
          "ethnicity",
          col("Race").alias("race"),
          col("Age").alias("age"),
          col("_metadata.file_path").alias("source_file"),
          current_timestamp().alias("processing_time")
        )
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))
