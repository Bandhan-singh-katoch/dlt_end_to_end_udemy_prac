from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.view(
    name= 'bs_vw_customers_gold'
)
def customers_gold_view():
    return spark.readStream.table("bs_vw_customers_silver")

dp.create_streaming_table(name="bs_dim_customers")

dp.create_auto_cdc_flow(
    target="bs_dim_customers",
    source="bs_vw_customers_gold",
    keys=["customer_id"],
    sequence_by="processDate",
    stored_as_scd_type=2,
    except_column_list=['processDate']
)