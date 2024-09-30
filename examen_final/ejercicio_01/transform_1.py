from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .master("spark://a00c3ef95998:7077")
    .enableHiveSupport()
    .getOrCreate()
)

df_1 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
df_2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")

df_12 = df_1.union(df_2)

df_12 = df_12.drop("Calidad dato")
df_12 = df_12.replace({"Doméstico": "Domestico"}, "Clasificación Vuelo")

final_df = df_12.select(
    to_date(col("Fecha"), "dd/MM/yyyy").alias("fecha"),
    col("Hora UTC").alias("horaUTC"),
    col("Clase de Vuelo (todos los vuelos)").alias("clase_de_vuelo"),
    col("Clasificación Vuelo").alias("clasificacion_de_vuelo"),
    col("Tipo de Movimiento").alias("tipo_de_movimiento"),
    col("Aeropuerto").alias("aeropuerto"),
    col("Origen / Destino").alias("origen_destino"),
    col("Aerolinea Nombre").alias("aerolinea_nombre"),
    col("Aeronave").alias("aeronave"),
    col("Pasajeros").cast("integer").alias("pasajeros")
).filter(
    col("clasificacion_de_vuelo") == "Domestico"
).fillna({"pasajeros": 0})

final_df.write.mode("overwrite").insertInto("aviacion.aeropuerto_tabla")

spark.stop()