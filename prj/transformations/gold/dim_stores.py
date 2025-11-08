from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.view(
    name= 'bs_vw_stores_gold'
)
def stores_gold_view():
    return spark.readStream.table("bs_vw_stores_silver")

dp.create_streaming_table(name="bs_dim_stores")

dp.create_auto_cdc_flow(
    target="bs_dim_stores",
    source="bs_vw_stores_gold",
    keys=["store_id"],
    sequence_by="processDate",
    stored_as_scd_type=2,
    except_column_list=['processDate']
)