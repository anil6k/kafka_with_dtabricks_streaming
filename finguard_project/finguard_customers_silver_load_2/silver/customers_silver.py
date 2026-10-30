ffrom pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
name = "fineguard.silver.customers"
,comment = "Parsed and cleaned customers data"

)

@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")

def customers_silver():
    bronze_df = spark.readStream.format("delta").table("fineguard.bronze.customers")

    transformed_df = bronze_df\
        .withColumn("account_open_date", F.to_date(F.col("account_open_date"), "yyyy-MM-dd"))\
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    return transformed_df
