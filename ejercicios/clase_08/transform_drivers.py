from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://a00c3ef95998:7077") \
    .enableHiveSupport() \
    .getOrCreate()

df_drivers = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/drivers.csv")
df_results = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")

df_drivers.createOrReplaceTempView("drivers")
df_results.createOrReplaceTempView("results")

result = spark.sql("""
    SELECT 
        CAST(d.forename AS STRING) AS driver_forename, 
        CAST(d.surname AS STRING) AS driver_surname, 
        CAST(d.nationality AS STRING) AS driver_nationality, 
        SUM(CAST(r.points AS FLOAT)) AS points
    FROM drivers d
    INNER JOIN results r ON r.driverId = d.driverId AND CAST(r.points AS FLOAT) > 0
    GROUP BY d.forename, d.surname, d.nationality
    ORDER BY points DESC
    LIMIT 20
""")

result.write.insertInto("formula1.driver_results")

spark.stop()