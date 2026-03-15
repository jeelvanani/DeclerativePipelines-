# import dlt

# #making streaming table
# @dlt.table(
#     name = "first_stream_table"
# )
# def first_stream_table():
#     df = spark.readStream.table("demo.demo.transactiontable")
#     return df


# #making materalized views

# @dlt.table(
#     name = "first_mat_view"
# )
# def first_mat_view():
#     df = spark.read.table("demo.demo.transactiontable")
#     return df


# #making normal view

# # in view 2 types. batch and streaming view

# #creating batch view
# @dlt.view(
#     name = "first_batch_view"
# )
# def first_batch_view():
#     df = spark.read.table("demo.demo.transactiontable")
#     return df


# #creating streaming view
# @dlt.view(
#     name = "first_streaming_view"
# )
# def first_streaming_view():
#     df = spark.readStream.table("demo.demo.transactiontable")
#     return df
