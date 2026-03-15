import dlt
from pyspark.sql.functions import *

#create the customers streaming view 
@dlt.view(
    name = "customers_enriched_view"
)
def customers_enriched_view():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("customer_name",upper(col("customer_name")))
    return df


#creating auto cdc flow for customers
dlt.create_streaming_table(
    name = "customers_enriched"
)

dlt.create_auto_cdc_flow(
  target = "customers_enriched",
  source = "customers_enriched_view",
  keys = ["customer_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None
)