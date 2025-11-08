from pyspark import pipelines as dp
from pyspark.sql.functions import *

# streaming view
@dp.view(
    name = 'bs_vw_sales_silver'
)
def sales_silver_view():
    df_sales = spark.readStream.table("bs_sales_bronze")
    df_sales = df_sales.withColumn("pricePerSales", round(col("total_amount")/col("quantity"),2))
    df_sales = df_sales.withColumn("processDate", current_timestamp())
    
    return df_sales

dp.create_streaming_table("bs_sales_silver")

dp.create_auto_cdc_flow(
    target="bs_sales_silver",
    source="bs_vw_sales_silver",
    keys=["sales_id"],
    sequence_by="processDate",
    stored_as_scd_type=1
)