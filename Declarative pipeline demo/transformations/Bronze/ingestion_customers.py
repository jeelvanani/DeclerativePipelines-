import dlt


#customers rules or expectation
customers_rules = {
    "rule_1": "region is not null",
    "rule_2": "customer_id is not null",
    "rule_3": "customer_name is not null"
}


@dlt.table(
    name = "customers_stg"
)
@dlt.expect_all(customers_rules)
def customers_stg():
    df = spark.readStream.table("demo.demo.customers")
    return df 