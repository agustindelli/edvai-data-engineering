from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://a00c3ef95998:7077") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv")

# Insertar en la tabla payments (VendorID, tpep_pickup_datetetime, payment_type, total_amount) Solamente los pagos con tarjeta de credito
payments_df = df.select(
    df.VendorID.cast("int"),
    df.tpep_pickup_datetime.cast("timestamp"),
    df.payment_type.cast("int"),
    df.total_amount.cast("float")
).filter(df.payment_type == 1)
payments_df.write.insertInto("tripdata.payments")

# Insertar en la tabla passengers (tpep_pickup_datetetime, passenger_count, total_amount) los registros cuya cantidad de pasajeros sea mayor a 2 y el total del viaje cueste mas de 8 dolares.
passengers_df = df.select(
    df.tpep_pickup_datetime.cast("timestamp"),
    df.passenger_count.cast("int"),
    df.total_amount.cast("float")
).filter((df.passenger_count > 1) & (df.total_amount > 8))
passengers_df.write.insertInto('tripdata.passengers')

# Insertar en la tabla tolls (tpep_pickup_datetetime, passenger_count, tolls_amount, total_amount) los registros que tengan pago de peajes mayores a 0.1 y cantidad de pasajeros mayores a 1.
tolls_df = df.select(
    df.tpep_pickup_datetime.cast("timestamp"),
    df.passenger_count.cast("int"),
    df.tolls_amount.cast("float"),
    df.total_amount.cast("float")
).filter((df.passenger_count > 1) & (df.tolls_amount > 0.1))
tolls_df.write.insertInto('tripdata.tolls')

# Insertar en la tabla congestion (tpep_pickup_datetetime, passenger_count, congestion_surcharge, total_amount) los registros que hayan tenido congestion en los viajes en la fecha 2021-01-18
congestion_df = df.select(
    df.tpep_pickup_datetime.cast("timestamp"),
    df.passenger_count.cast("int"),
    df.congestion_surcharge.cast("float"),
    df.total_amount.cast("float")
).filter((df.tpep_pickup_datetime.cast("date") == '2021-01-18') & (df.congestion_surcharge > 0))
congestion_df.write.insertInto("tripdata.congestion")

# Insertar en la tabla distance (tpep_pickup_datetetime, passenger_count, trip_distance, total_amount) los registros de la fecha 2020-12-31 que hayan tenido solamente un pasajero (passenger_count = 1) y hayan recorrido mas de 15 millas (trip_distance).
distance_df = df.select(
    df.tpep_pickup_datetime.cast("timestamp"),
    df.passenger_count.cast("int"),
    df.trip_distance.cast("float"),
    df.total_amount.cast("float")
).filter((df.tpep_pickup_datetime.cast("date") == '2020-12-31') & (df.passenger_count == 1) & (df.trip_distance > 15))
distance_df.write.insertInto("tripdata.distance")

spark.stop()