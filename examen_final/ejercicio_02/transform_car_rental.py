from pyspark.sql import SparkSession
from pyspark.sql.functions import round, col, lower 

spark = (
    SparkSession.builder
    .master("spark://a00c3ef95998:7077")
    .enableHiveSupport()
    .getOrCreate()
)

df_1 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
df_2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")

def format_columns(df):
    new_columns = [column.replace('.', '_').replace(' ', '_') for column in df.columns]
    return df.toDF(*new_columns)

df_1 = format_columns(df_1)
df_1 = (
    df_1
    .dropna(subset=["rating"])
    .drop("location_country", "location_latitude", "location_longitude", "vehicle_type")
    .withColumn("fuelType", lower(col("fuelType")))
    .withColumn("rating", round(col("rating")))
)

df_2 = format_columns(df_2)
df_2 = (
    df_2
    .withColumnRenamed("United_States_Postal_Service_state_abbreviation", "state_abbr")
    .withColumnRenamed("Official_Name_State", "state_name")
    .select("state_abbr", "state_name")
    .filter(col("state_abbr") != "TX") # se filtran los regs en el join (inner)
)

df_3 = df_1.join(df_2, col("location_state") == col("state_abbr"), 'inner')

final_df = df_3.select(
    col("fuelType"),
    col("rating").cast("integer"),
    col("renterTripsTaken").cast("integer"),
    col("reviewCount").cast("integer"),
    col("location_city").alias("city"),
    col("state_name"),
    col("owner_id").cast("integer"),
    col("rate_daily").cast("integer"),
    col("vehicle_make").alias("make"),
    col("vehicle_model").alias("model"),
    col("vehicle_year").cast("integer").alias("year")
)

final_df.write.mode("overwrite").insertInto("car_rental_db.car_rental_analytics")

spark.stop()