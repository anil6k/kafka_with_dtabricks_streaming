# Databricks notebook source
source_path = '/Volumes/fineguard/source/fraud_watchlist/source_data/'

# COMMAND ----------

dbutils.fs.ls(source_path)

# COMMAND ----------

# DBTITLE 1,Auto Loader JSON stream
input_stream = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/Volumes/fineguard/source/fraud_watchlist/schema_json/")
  .option("cloudFiles.inferColumnTypes", "true")
  .load(source_path))

# COMMAND ----------

from pyspark.sql import functions as F
transformed_df = input_stream.select(
    "*",
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingest_timestamp")                                     
                                     
                                     )

# COMMAND ----------

streaming_query = (transformed_df.writeStream.format("delta")
                                .outputMode("Append")
                                .option("checkpointLocation", "/Volumes/fineguard/source/fraud_watchlist/checkpoint/")
                                .trigger(availableNow=True)
                                .toTable("fineguard.bronze.fraud_watchlist_batch_test")
                                )



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fineguard.bronze.fraud_watchlist