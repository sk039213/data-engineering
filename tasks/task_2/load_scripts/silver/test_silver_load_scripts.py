import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable

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
    StructField("updated_at", TimestampType(), True)  # Add updated_at column
])

script_name = "avocado_silver"
spark = SparkSession.builder.appName("AvocadoSilverLayer").getOrCreate()

bronze_table = "data_engineering.bronze.avocado"
silver_table = "data_engineering.silver.avocado"

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

# Ensure the schema matches before merging
df_clean = df_clean.select([col(field.name) for field in schema.fields])
display(df_clean)

if not spark.catalog.tableExists(silver_table):
    df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    # Merge into Silver table using Delta Lake's merge functionality
    delta_table = DeltaTable.forName(spark, silver_table)
    (delta_table.alias("t")
        .merge(
            df_clean.alias("s"),
            "t.consumer_id = s.consumer_id and t.purchase_id = s.purchase_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
