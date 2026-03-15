import dlt
from pyspark.sql.functions import * 

#creating product streaming view
@dlt.view(
    name = "products_enriched_view"
)
def products_enriched_view():
    df = spark.readStream.table("products_stg")
    df = df.withColumn("price", col("price").cast("int"))
    return df


#creating auto cdc flow for silver layer:
dlt.create_streaming_table(
    name = "products_enriched"
)

dlt.create_auto_cdc_flow(
  target = "products_enriched",
  source = "products_enriched_view",
  keys = ["product_id"],
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