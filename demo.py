from pyspark.sql.functions import monotonically_increasing_id, rand, col, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define schema for realistic scenario (e.g., customer transactions)
schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("location", StringType(), False),
    StructField("timestamp", StringType(), False)
])

# Generate base DataFrame with 1000 rows
df = spark.range(1000).withColumnRenamed("id", "transaction_id")

# Add realistic columns
df = df.withColumn("customer_id", (col("transaction_id") % 200) + 1) \
       .withColumn("product", expr("CASE WHEN transaction_id % 5 = 0 THEN 'Laptop' WHEN transaction_id % 5 = 1 THEN 'Phone' WHEN transaction_id % 5 = 2 THEN 'Tablet' WHEN transaction_id % 5 = 3 THEN 'Headphones' ELSE 'Camera' END")) \
       .withColumn("amount", (rand() * 1000 + 50).cast("double")) \
       .withColumn("location", expr("CASE WHEN transaction_id % 4 = 0 THEN 'New York' WHEN transaction_id % 4 = 1 THEN 'San Francisco' WHEN transaction_id % 4 = 2 THEN 'Chicago' ELSE 'Austin' END")) \
       .withColumn("timestamp", expr("date_add('2026-03-01', transaction_id % 14)"))

# Apply schema
df = spark.createDataFrame(df.rdd, schema)

display(df)