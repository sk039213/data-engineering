# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# stored customer's raw data into dbfs(data bricks file storage)
print(dbutils.fs.ls("dbfs:/FileStore/tables/bronze"))

# Load the raw data into respective data frames.

# File extension type
file_type = "csv"
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
avocado_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load("dbfs:/FileStore/tables/bronze/avocado.csv")
)

consumer_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load("dbfs:/FileStore/tables/bronze/consumer.csv")
)

purchase_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load("dbfs:/FileStore/tables/bronze/purchase.csv")
)

fertilizer_df = (
    spark.read.format(file_type)
    .option("inferSchema", infer_schema)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load("dbfs:/FileStore/tables/bronze/fertilizer.csv")
)

# COMMAND ----------

# check/verify schema of all entities

avocado_df.printSchema()
consumer_df.printSchema()
purchase_df.printSchema()
fertilizer_df.printSchema()

# COMMAND ----------

# Consumer data issues

display(consumer_df)
# 1. Age is not valued for one consumer.
# 2. Age is inappropriate for another consumer.

# COMMAND ----------

# Purchase data issues

display(purchase_df)

# 1. graphed_date column data appears incorrect related to the reporting_year specified.

# COMMAND ----------

# Avocado entity data issues

display(avocado_df)
# 1. picked_date column format is different than the born_date, sold_date columns.
# 2. picked_date data in some of the rows is out of range of born_date & sold_date

# COMMAND ----------

# Row counts
print("Avocado row count: ", avocado_df.count())
print("Consumer row count: ", consumer_df.count())
print("Purchase row count: ", purchase_df.count())
print("Fertilizer row count: ", fertilizer_df.count())

# # Check for duplicates with respect to the primary key of each file
print("Avocado duplicates:", avocado_df.count() - avocado_df.dropDuplicates().count())
print(
    "Consumer duplicates:", consumer_df.count() - consumer_df.dropDuplicates().count()
)
print(
    "Fertilizer duplicates:",
    fertilizer_df.count() - fertilizer_df.dropDuplicates().count(),
)
print(
    "Purchase duplicates:", purchase_df.count() - purchase_df.dropDuplicates().count()
)

# COMMAND ----------

# Drop duplicate rows in fertilizer data frame.

print("fertilizer row count original:", fertilizer_df.count())
fertilizer_df = fertilizer_df.drop_duplicates()
print("fertilizer row count post duplicates removal:", fertilizer_df.count())

# COMMAND ----------

# Drop null rows in purchase data frame.

# Filter out rows with null primary & foreign key values
print("purchase row count original:", purchase_df.count())
non_null_rows = purchase_df.purchaseid.isNotNull() & purchase_df.consumerid.isNotNull()
purchase_df = purchase_df[non_null_rows]
print("purchase row count after:", purchase_df.count())
display(purchase_df)

# COMMAND ----------

# Verify null rows in fertilizer data frame.

print("fertilizer df row count:", fertilizer_df.count())
non_null_rows = fertilizer_df.fertilizerid.isNotNull()
not_null_df = fertilizer_df[non_null_rows]
print("non null row count:", not_null_df.count())

# COMMAND ----------

# Standardize Avocado schema i.e, convert column names to lower case and apply snake case format between words.

from pyspark.sql.functions import datediff, col

avocado_df = avocado_df.withColumnRenamed("consumerid", "consumer_id")
avocado_df = avocado_df.withColumnRenamed("purchaseid", "purchase_id")
avocado_df = avocado_df.withColumnRenamed("PLU", "plu")
avocado_df = avocado_df.withColumnRenamed(
    "ripe index when picked", "avocado_ripe_index"
)
avocado_df = avocado_df.withColumnRenamed("born_date", "born_at")
avocado_df = avocado_df.withColumnRenamed("picked_date", "picked_at")
avocado_df = avocado_df.withColumnRenamed("sold_date", "sold_at")
avocado_df = avocado_df.withColumn(
    "avocado_days_old", datediff(col("sold_at"), col("born_at"))
)

# drop bad data rows i.e, picked_at is not in between born_at & sold_at
avocado_df = avocado_df[
    (avocado_df.picked_at > avocado_df.born_at)
    & (avocado_df.picked_at <= avocado_df.sold_at)
]

display(avocado_df)

avocado_df = avocado_df.withColumn(
    "avocado_days_picked", datediff(col("sold_at"), col("picked_at"))
)

display(avocado_df)

avocado_df.printSchema()

# COMMAND ----------

# Standardize Consumer schema i.e, column names

consumer_df = consumer_df.withColumnRenamed("consumerid", "consumer_id")
consumer_df = consumer_df.withColumnRenamed("Sex", "sex")
consumer_df = consumer_df.withColumnRenamed("Race", "race")
consumer_df = consumer_df.withColumnRenamed("Age", "age")

display(consumer_df)

consumer_df.printSchema()

# COMMAND ----------

# Standardize Fertilizer schema i.e, column names

fertilizer_df = fertilizer_df.withColumnRenamed("fertilizerid", "fertilizer_id")
fertilizer_df = fertilizer_df.withColumnRenamed("consumerid", "consumer_id")
fertilizer_df = fertilizer_df.withColumnRenamed("purchaseid", "purchase_id")
fertilizer_df = fertilizer_df.withColumnRenamed("type", "fertilizer_type")

display(fertilizer_df)

fertilizer_df.printSchema()

# COMMAND ----------

# Standardize Purchase schema i.e, column names

purchase_df = purchase_df.withColumnRenamed("consumerid", "consumer_id")
purchase_df = purchase_df.withColumnRenamed("purchaseid", "purchase_id")
purchase_df = purchase_df.withColumnRenamed("QA process", "qa_process")

display(purchase_df)

purchase_df.printSchema()

# COMMAND ----------

display(avocado_df)
display(consumer_df)
display(fertilizer_df)
display(purchase_df)

# COMMAND ----------

# Join the tables for generating the final output data frame
output_df = (
    purchase_df.alias("p")
    .join(consumer_df, on=["consumer_id"], how="left")
    .join(fertilizer_df, on=["purchase_id", "consumer_id"], how="left")
    .join(avocado_df, on=["purchase_id", "consumer_id"], how="left")
    .select(
        col("p.consumer_id"),
        col("sex"),
        col("age"),
        col("avocado_days_old"),
        col("avocado_ripe_index"),
        col("avocado_days_picked"),
        col("fertilizer_type"),
    )
)

display(output_df)

# COMMAND ----------

# Write output data frame to file storage system

from datetime import datetime

# Get the current date
current_date = datetime.now().date()
display(output_df)
# Write the output to a CSV file
iteration = 1
output_df.write.format("csv") \
    .option("delimiter", "|") \
    .option("lineSep", "\n") \
    .option("quote", "'") \
    .option("encoding", "ASCII") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"dbfs:/FileStore/tables/bronze/target_{iteration}_{current_date}.csv")

# COMMAND ----------

# Verify the output file in file storage system

# dbutils.fs.ls("dbfs:/FileStore/tables/bronze")

written_output_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("dbfs:/FileStore/tables/bronze/target_1_2024-09-04.csv")
)

display(written_output_df)
