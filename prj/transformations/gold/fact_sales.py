from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.view(
    name= 'bs_vw_sales_gold'
)
def sales_gold_view():
    return spark.readStream.table("bs_vw_sales_silver")

dp.create_streaming_table(name="bs_fact_sales")

dp.create_auto_cdc_flow(
    target="bs_fact_sales",
    source="bs_vw_sales_gold",
    keys=["sales_id"],
    sequence_by="processDate",
    stored_as_scd_type=1
)