from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://a00c3ef95998:7077") \
    .enableHiveSupport() \
    .getOrCreate()
    
df_1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")
df_2 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")

df = df_1.union(df_2)

filtered_df = df.select(
    df.tpep_pickup_datetime.cast("timestamp"),
    df.airport_fee.cast("float"),
    df.payment_type.cast("integer"),
    df.tolls_amount.cast("float"),
    df.total_amount.cast("float")
).filter( (df.airport_fee > 0) & (df.payment_type == 2) )
filtered_df.write.insertInto("tripdata.airport_trips")

spark.stop()