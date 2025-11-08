from pyspark import pipelines as dp
from pyspark.sql.functions import *

# streaming view
@dp.view(
    name = 'bs_vw_stores_silver'
)
def stores_silver_view():
    df_stores = spark.readStream.table("bs_stores_bronze")
    df_stores = df_stores.withColumn("store_name", regexp_replace(col("store_name"), "_", ""))    
    df_stores = df_stores.withColumn("processDate", current_timestamp())
    return df_stores

dp.create_streaming_table("bs_stores_silver")

dp.create_auto_cdc_flow(
    target="bs_stores_silver",
    source="bs_vw_stores_silver",
    keys=["store_id"],
    sequence_by="processDate",
    stored_as_scd_type=1
)