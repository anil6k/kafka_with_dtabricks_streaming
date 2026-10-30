from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

@dp.table(
name = "fineguard.bronze.fraud_watchlist"
,comment = "fraud watchlist raw stream data ingested by  auto loader"

)

def fraud_watchlist_bronze() -> DataFrame:


    streaming_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/fineguard/source/fraud_watchlist/source_data/")
        )

    parsed_streaming_df = streaming_df.select(col("watchlist_id"),
                                              col("watch_type"),
                                              col("entity_id"),
                                              col("risk_level"),
                                              col("action"),
                                              col("reason_code"),
                                              col("reason_description"),
                                              col("status"),
                                              col("effective_from"),
                                              col("reported_by"),
                                              col("reported_source"),
                                              col("city"),
                                              col("country"),
                                              col("_rescued_data"),
                                              col("_metadata.file_path").alias("source_file"),
                                              
                                 
                                     F.current_timestamp().alias("ingestion_timestamp")
                                   )

    return parsed_streaming_df
