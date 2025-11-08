from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.view(
    name= 'bs_vw_products_gold'
)
def _products_gold_view():
    return spark.readStream.table("bs_vw_products_silver")

dp.create_streaming_table(name="bs_dim__products")

dp.create_auto_cdc_flow(
    target="bs_dim__products",
    source="bs_vw_products_gold",
    keys=["product_id"],
    sequence_by="processDate",
    stored_as_scd_type=2,
    except_column_list=['processDate']
)