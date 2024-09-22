from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://a00c3ef95998:7077") \
    .enableHiveSupport() \
    .getOrCreate()

df_drivers = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/drivers.csv")
df_results = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")
df_constructors = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/constructors.csv")
df_races = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/races.csv")

df_drivers.createOrReplaceTempView("drivers")
df_results.createOrReplaceTempView("results")

result_1 = spark.sql("""
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

df_constructors.createOrReplaceTempView("constructors")
df_races.createOrReplaceTempView("races")

result_2 = spark.sql("""
    SELECT 
        CAST(c.constructorRef AS STRING) AS constructorRef, 
        CAST(c.name AS STRING) AS cons_name, 
        CAST(c.nationality AS STRING) AS cons_nationality, 
        CAST(c.url AS STRING) AS url,
        SUM(CAST(r.points AS FLOAT)) AS points
    FROM constructors c
    INNER JOIN results r ON r.constructorId = c.constructorId AND CAST(r.points AS FLOAT) > 0
    INNER JOIN races ra ON ra.raceId = r.raceId AND ra.name = 'Spanish Grand Prix' AND ra.year = 1991
    GROUP BY c.constructorRef, c.name, c.nationality, c.url
    ORDER BY points DESC
""")

result_1.write.insertInto("formula1.driver_results")
result_2.write.insertInto("formula1.constructor_results")

spark.stop()
