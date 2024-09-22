from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://a00c3ef95998:7077") \
    .enableHiveSupport() \
    .getOrCreate()

df_constructors = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/constructors.csv")
df_results = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")
df_races = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/races.csv")

df_constructors.createOrReplaceTempView("constructors")
df_results.createOrReplaceTempView("results")
df_races.createOrReplaceTempView("races")

result = spark.sql("""
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

result.write.insertInto("formula1.constructor_results")

spark.stop()