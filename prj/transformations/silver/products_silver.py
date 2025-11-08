from pyspark import pipelines as dp
from pyspark.sql.functions import *

# streaming view
@dp.view(
    name = 'bs_vw_products_silver'
)
def products_silver_view():
    df_products = spark.readStream.table("bs_products_bronze")
    df_products = df_products.withColumn("processDate", current_timestamp())
    
    return df_products

dp.create_streaming_table("bs_products_silver")

dp.create_auto_cdc_flow(
    target="bs_products_silver",
    source="bs_vw_products_silver",
    keys=["product_id"],
    sequence_by="processDate",
    stored_as_scd_type=1
)