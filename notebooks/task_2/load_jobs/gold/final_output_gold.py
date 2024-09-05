from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_timestamp
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.appName("gold_layer").getOrCreate()

# Table names
silver_avocado_table = "data_engineering.avocado_silver"
silver_consumer_table = "data_engineering.consumer_silver"
silver_fertilizer_table = "data_engineering.fertilizer_silver"
silver_purchase_table = "data_engineering.purchase_silver"
gold_table = "data_engineering.output_gold"

# Read data from the Silver tables
consumer_df = spark.read.format("delta").table(silver_consumer_table)
purchase_df = spark.read.format("delta").table(silver_purchase_table)
avocado_df = spark.read.format("delta").table(silver_avocado_table)
fertilizer_df = spark.read.format("delta").table(silver_fertilizer_table)

# Perform necessary transformations
avocado_df = avocado_df.withColumn("avocado_days_old", datediff(col("sold_at"), col("born_at")))
avocado_df = avocado_df.withColumnRenamed("ripe_index_when_picked", "avocado_ripe_index")
avocado_df = avocado_df.withColumn("avocado_days_picked", datediff(col("sold_at"), col("picked_at")))

fertilizer_df = fertilizer_df.withColumnRenamed("type", "fertilizer_type")

# Join the tables and drop the join columns to avoid ambiguity
consumer_purchase_df = consumer_df.alias("c") \
    .join(purchase_df.alias("p"), "consumer_id", how="outer") \
    .drop(purchase_df["consumer_id"])

consumer_purchase_avocado_df = consumer_purchase_df.alias("cp") \
    .join(avocado_df.alias("a"), ["purchase_id", "consumer_id"], how="outer") \
    .drop(avocado_df["purchase_id"]) \
    .drop(avocado_df["consumer_id"])

final_df = consumer_purchase_avocado_df.alias("cpa") \
    .join(fertilizer_df.alias("f"), ["purchase_id", "consumer_id"], how="outer") \
    .drop(fertilizer_df["purchase_id"])
    .drop(fertilizer_df["consumer_id"])

# Select the required columns and add updated_at column
output_df = final_df.select(
    col("cpa.consumerid").alias("consumer_id"),
    col("cpa.sex"),
    col("cpa.age"),
    col("cpa.avocado_days_old"),
    col("cpa.avocado_ripe_index"),
    col("cpa.avocado_days_picked"),
    col("f.fertilizer_type")
).withColumn("updated_at", current_timestamp())

# Filter out records with non-positive values for avocado_days_old and avocado_days_picked
output_df = output_df.filter(
    (col("avocado_days_old") > 0) & (col("avocado_days_picked") > 0)
)

# Ensure the schema matches before merging
output_df = output_df.select(
    col("consumer_id").cast("long"),
    col("sex").cast("string"),
    col("age").cast("integer"),
    col("avocado_days_old").cast("integer"),
    col("avocado_ripe_index").cast("integer"),
    col("avocado_days_picked").cast("integer"),
    col("fertilizer_type").cast("string"),
    col("updated_at").cast("timestamp")
).dropDuplicates()

# Check if the Gold table exists
if not spark.catalog.tableExists(gold_table):
    output_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
else:
    # Merge into Gold table using Delta Lake's merge functionality
    delta_table = DeltaTable.forName(spark, gold_table)
    delta_table.alias("t") \
        .merge(
            output_df.alias("s"),
            "t.consumer_id = s.consumer_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

print(f"Data upserted to Gold table: {gold_table}")


