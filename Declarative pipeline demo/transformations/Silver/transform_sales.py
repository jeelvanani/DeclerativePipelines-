import dlt
from pyspark.sql.functions import *

#creating view to transforming sales data
@dlt.view(
    name = "sales_enriched_view"
)
def sales_enriched_view():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("total_amount", col("amount") * col("quantity"))
    return df


#crearing desination silver table
dlt.create_streaming_table(
    name = "sales_enriched"
)

dlt.create_auto_cdc_flow(
  target = "sales_enriched",
  source = "sales_enriched_view",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None
)




