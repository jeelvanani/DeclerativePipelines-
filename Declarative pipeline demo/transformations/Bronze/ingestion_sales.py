import dlt

#sales expectation
sales_rules = {
    "rule_1" : "sales_id is not null"
}

#create empty streaming table
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop = sales_rules
)

#creating East sales flow. appending into the empty table
@dlt.append_flow(target="sales_stg")
def east_sales():
    df = spark.readStream.table("demo.demo.sales_east")
    return df

# creating West sales flow. appending on the east sales
@dlt.append_flow(target = "sales_stg")
def west_sales():
    df = spark.readStream.table("demo.demo.sales_west")
    return df

    