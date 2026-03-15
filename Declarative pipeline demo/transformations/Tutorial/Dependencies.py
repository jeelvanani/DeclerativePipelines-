# #creating an end-to-end basic pipeline

# import dlt


# # staging area
# @dlt.table(
#     name = "stagingTransaction"
# )
# def stagingTransaction():
#     df = spark.readStream.table("demo.demo.transactiontable")
#     return df