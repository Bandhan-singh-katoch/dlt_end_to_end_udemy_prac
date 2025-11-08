from pyspark import pipelines as dp
from pyspark.sql.functions import *

# streaming view
@dp.view(
    name = 'bs_vw_customers_silver'
)
def customers_silver_view():
    df_customers = spark.readStream.table("bs_customers_bronze")
    df_customers = df_customers.withColumn('name', upper(col("name")))
    df_customers = df_customers.withColumn('domain',split(col("email"), "@")[1])

    df_customers = df_customers.withColumn("processDate", current_timestamp())
    
    return df_customers

dp.create_streaming_table("bs_customers_silver")

dp.create_auto_cdc_flow(
    target="bs_customers_silver",
    source="bs_vw_customers_silver",
    keys=["customer_id"],
    sequence_by="processDate",
    stored_as_scd_type=1
)