import dlt
from pyspark.sql.functions import *

#creating materalized business view
@dlt.table(
    name = "business_sales"
)
def buisness_sales():
    df_fact = spark.read.table("fact_sales")
    df_dimCust = spark.read.table("dim_customers")
    df_dimProd = spark.read.table("dim_products")
    
    df_join = df_fact.join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id, "inner").join(df_dimProd, df_fact.product_id == df_dimProd.product_id, "inner")
    
    df_pruned = df_join.select("region", "category" , "total_amount")

    df_agg = df_pruned.groupBy("region", "category").agg(sum("total_amount").alias("total_sales"))
    
    return df_agg