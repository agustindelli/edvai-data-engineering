from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .master("spark://a00c3ef95998:7077")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

df = df.drop("inhab", "fir")

final_df = df.select(
    col("local").alias("aeropuerto"),
    col("oaci").alias("oac"),
    col("iata"),
    col("tipo"),
    col("denominacion"),
    col("coordenadas"),
    col("latitud"),
    col("longitud"),
    col("elev").cast("float").alias("elev"),
    col("uom_elev"),
    col("ref"),
    col("distancia_ref").cast("float").alias("distancia_ref"),
    col("direccion_ref"),
    col("condicion"),
    col("control"),
    col("region"),
    col("uso"),
    col("trafico"),
    col("sna"),
    col("concesionado"),
    col("provincia")
).fillna({"distancia_ref": 0.0})

final_df.write.mode("overwrite").insertInto("aviacion.aeropuerto_detalles_tabla")

spark.stop()