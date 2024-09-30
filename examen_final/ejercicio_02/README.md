# Ejercicio 2

Ejercicio 2 del examen final del curso

##  Alquiler de automóviles

Una de las empresas líderes en alquileres de automóviles solicita una serie de dashboards y reportes para poder basar sus decisiones en datos. Entre los indicadores mencionados se
encuentran total de alquileres, segmentación por tipo de combustible, lugar, marca y modelo de automóvil, valoración de cada alquiler, etc. Como Data Engineer debe crear y automatizar el pipeline para tener como resultado los datos listos para ser visualizados y responder las preguntas de negocio.

### Tareas:

#### 1. Crear en hive una database car_rental_db y dentro una tabla llamada car_rental_analytics, con estos campos:

#### **Tabla - car_rental_analytics**

| **Campos**             | **Tipo**   |
|------------------------|------------|
| fuelType               | integer    |
| rating                 | integer    |
| renterTripsTaken       | integer    |
| reviewCount            | string     |
| city                   | string     |
| state_name             | string     |
| owner_id               | integer    |
| rate_daily             | integer    |
| make                   | string     |
| model                  | string     |
| year                   | integer    |

`hive_create_tables.sh`

```bash
#!/bin/bash

hive -e "
CREATE DATABASE IF NOT EXISTS car_rental_db;

USE car_rental_db;

CREATE EXTERNAL TABLE IF NOT EXISTS car_rental_analytics (
	fuelType STRING, 
	rating INTEGER, 
	renterTripsTaken INTEGER, 
	reviewCount INTEGER, 
	city STRING, 
	state_name STRING, 
	owner_id INTEGER, 
	rate_daily INTEGER, 
	make STRING, 
	model STRING, 
	year INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/tables/external/car_rental_analytics';
"
```

#### 2. Crear script para el ingest de estos dos files.

https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv

https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

`ingest.sh`

```bash
#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL1="https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv"
URL2="https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"

rm -f ${LANDING_DIR}/*

wget -P ${LANDING_DIR} ${URL1} ${URL2}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/* ${INGEST_DIR}

rm -f ${LANDING_DIR}/*
```

#### 3. Crear un script para tomar el archivo desde HDFS y hacer las siguientes transformaciones:
- Endondeseanecesario, modificar los nombres de las columnas. Evitar espacios y puntos (reemplazar por _ ). Evitar nombres de columna largos
- Redondear los float de 'rating' y castear a int.
- Joinearambosfiles
- Eliminarlosregistros con rating nulo
- Cambiarmayúsculaspor minúsculas en 'fuelType'
- Excluir el estado Texas
  
`transform_car_rental.py`

```python
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
```

####  4. Realizar un proceso automático en Airflow que orqueste los pipelines creados en los puntos anteriores. Crear dos tareas:
- Un DAG padre que ingente los archivos y luego llame al DAG hijo
- Un DAG hijo que procese la información y la cargue en Hive

`car_rental_ingest_dag.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="car_rental_ingest_dag",
    default_args=args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "transform_trigger"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    
    ingest = BashOperator(
        task_id="ingest",
        bash_command="/usr/bin/sh /home/hadoop/scripts/examen_final/ejercicio2/ingest.sh ",
    )

    transform_trigger = TriggerDagRunOperator(
        task_id="transform_trigger",
        trigger_dag_id="car_rental_transform_dag",
    )
    
    end_process = DummyOperator(task_id="end_process")

    start_process >> ingest >> transform_trigger >> end_process
```

`car_rental_transform_dag.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="car_rental_transform_dag",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["transform"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    
    transform = BashOperator(
        task_id="transform",
        bash_command="ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio2/transform_car_rental.py ",
    )

    end_process = DummyOperator(task_id="end_process")

    start_process >> transform >> end_process
```

#### 5. Por medio de consultas SQL al data-warehouse, mostrar:

- Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4

```sql
SELECT 
	count(1) as ecologicos
FROM car_rental_analytics
WHERE fueltype in ('hybrid', 'electric')
AND rating >=4
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/5a.png)

- Los 5 estados con menor cantidad de alquileres (mostrar query y visualización)

```sql
SELECT 
	state_name, 
	sum(rentertripstaken) as alquileres 
FROM car_rental_analytics 
GROUP BY state_name 
ORDER BY alquileres
LIMIT 5;
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/5b.png)
  
- Los 10 modelos (junto con su marca) de autos más rentados (mostrar query y visualización)

```sql
SELECT 
	make, 
	model,
	sum(rentertripstaken) as alquileres 
FROM car_rental_analytics 
GROUP BY make, model 
ORDER BY alquileres DESC
LIMIT 10;
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/5c.png)

- Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015

```sql
SELECT 
	year, 
	COUNT(rentertripstaken) as alquileres 
FROM car_rental_analytics 
WHERE 
	year BETWEEN 2010 AND 2015
GROUP BY year
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/5d.png)

- Las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)

```sql
SELECT 
	city, 
	COUNT(rentertripstaken) as alquileres
FROM car_rental_analytics 
WHERE 
	fueltype in ('hybrid','electric') 
GROUP BY city 
ORDER BY alquileres DESC 
LIMIT 5;
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/5e.png)

- El promedio de reviews, segmentando por tipo de combustible
  
```sql
SELECT 
	fueltype, 
	avg(reviewcount) as promedio_reviews
FROM car_rental_analytics
GROUP BY fueltype
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/5f.png)

#### 6. Elabore sus conclusiones y recomendaciones sobre este proyecto.
Se puede recomendar sobre este proyecto que si se observa mucha demanda y buenas calificaciones para cierto tipo de autos, sería óptimo aumentar la disponibilidad de estos modelos y priorizarlo por sobre otros. También es importante entender qué está causando la baja cantidad de alquileres en algunos estados y tomar medidas para mejorar el servicio en esas áreas, como puede ser mayor publicidad y mejoras en la oferta. Es recomendable tener suficientes unidades de las mas populares y solicitadas ya que son los que generan mas ingresos. Es importante fomentar a que se realizen mas reseñas para sacar mas provecho y explotar mejor los datos. Por último, Una crítica al dataset que se podría realizar para mejorar la calidad de los datos es la columna fuelType, la cual tiene muchos nulos, lo que podría afectar negativamente el análisis y la toma de decisiones.

#### 7. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Si aplica).
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_02/captures/diagram.png)