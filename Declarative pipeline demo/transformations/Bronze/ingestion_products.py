import dlt

#products expectations
product_rules = {
    "rule_1": "Product_id IS NOT NULL",
    "rule_2": "price >= 0"
}

# ingesting products
@dlt.table(
    name = "products_stg"
)
@dlt.expect_all(product_rules)
def products_stg():
    df = spark.readStream.table("demo.demo.products")
    return df

