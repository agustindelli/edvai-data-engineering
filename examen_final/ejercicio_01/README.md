# Ejercicio 1

Ejercicio 1 del examen final del curso

## Aviación Civil
La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino, como puede ser: cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades de partidas y aterrizajes entre fechas determinadas, etc. Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y hacer sus recomendaciones con respecto al estado actual.

Listado de vuelos realizados: 

https://datos.gob.ar/lv/dataset/transporte-aterrizajes-despegues-procesados-por-administracion-nacional-aviacion-civil-anac

Listado de detalles de aeropuertos de Argentina: 

https://datos.transporte.gob.ar/dataset/lista-aeropuertos

### Tareas:

#### 1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina:

2021: https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

2022: https://dataengineerpublic.blob.core.windows.net/data-engineer202206-informe-ministerio.csv

Aeropuertos_detalles: https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

`ingest.sh`

```bash
#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL1="https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv"
URL2=" https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv"
URL3=" https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv"

rm -f ${LANDING_DIR}/*

wget -P ${LANDING_DIR} ${URL1} ${URL2} ${URL3}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/* ${INGEST_DIR}

rm -f ${LANDING_DIR}/*
```

####  2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022 (2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de los aeropuertos (aeropuertos_detalle.csv).

#### **Tabla 1 - aeropuerto_tabla**

| **Campos**             | **Tipo**   |
|------------------------|------------|
| fecha                  | date       |
| horaUTC                | string     |
| clase_de_vuelo         | string     |
| clasificacion_de_vuelo | string     |
| tipo_de_movimiento     | string     |
| aeropuerto             | string     |
| origen_destino         | string     |
| aerolinea_nombre       | string     |
| aeronave               | string     |
| pasajeros              | integer    |

#### **Tabla 2 - aeropuerto_detalles_tabla**

| **Campos**           | **Tipo**   |
|----------------------|------------|
| aeropuerto           | string     |
| oac                  | string     |
| iata                 | string     |
| tipo                 | string     |
| denominacion         | string     |
| coordenadas          | string     |
| latitud              | string     |
| longitud             | string     |
| elev                 | float      |
| uom_elev             | string     |
| ref                  | string     |
| distancia_ref        | float      |
| direccion_ref        | string     |
| condicion            | string     |
| control              | string     |
| region               | string     |
| uso                  | string     |
| trafico              | string     |
| sna                  | string     |
| concesionado         | string     |
| provincia            | string     |

`hive_create_tables.sh`

```bash
#!/bin/bash

hive -e "
CREATE DATABASE IF NOT EXISTS aviacion;

USE aviacion;

CREATE EXTERNAL TABLE IF NOT EXISTS aeropuerto_tabla (
    fecha DATE, 
    horaUTC STRING, 
    clase_de_vuelo STRING, 
    clasificacion_de_vuelo STRING, 
    tipo_de_movimiento STRING, 
    aeropuerto STRING, 
    origen_destino STRING, 
    aerolinea_nombre STRING, 
    aeronave STRING, 
    pasajeros INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/tables/external/aeropuerto_tabla';

CREATE EXTERNAL TABLE IF NOT EXISTS aeropuerto_detalles_tabla (
    aeropuerto STRING, 
    oac STRING, 
    iata STRING, 
    tipo STRING, 
    denominacion STRING, 
    coordenadas STRING, 
    latitud STRING, 
    longitud STRING, 
    elev FLOAT, 
    uom_elev STRING, 
    ref STRING, 
    distancia_ref FLOAT, 
    direccion_ref STRING, 
    condicion STRING, 
    control STRING, 
    region STRING, 
    uso STRING, 
    trafico STRING, 
    sna STRING, 
    concesionado STRING, 
    provincia STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '/tables/external/aeropuerto_detalles_tabla';
"
```

####  3. Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos columnas creadas. 
Los archivos 202206-informe-ministerio.csv y 202206-informe-ministerio.csv → en la tabla aeropuerto_tabla.

El archivo aeropuertos_detalle.csv → en la tabla aeropuerto_detalles_tabla.

`aviacion_etl_dag.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aviacion_etl_dag",
    default_args=args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "transform"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    
    ingest = BashOperator(
        task_id="ingest",
        bash_command="/usr/bin/sh /home/hadoop/scripts/examen_final/ejercicio1/ingest.sh ",
    )

    with TaskGroup("transform") as transform:

        transform_1 = BashOperator(
            task_id="transform_1",
            bash_command="ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio1/transform_1.py ",
        )

        transform_2 = BashOperator(
            task_id="transform_2",
            bash_command="ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio1/transform_2.py ",
        )

    end_process = DummyOperator(task_id="end_process")

    start_process >> ingest >> transform >> end_process
```

#### 4. Realizar las siguiente transformaciones en los pipelines de datos:
- Eliminar la columna inhab ya que no se utilizará para el análisis
- Eliminar la columna fir ya que no se utilizará para el análisis
- Eliminar la columna “calidad del dato” ya que no se utilizará para el análisis
- Filtrar los vuelos internacionales ya que solamente se analizarán los vuelos domésticos
- En elcampopasajeros si se encuentran campos en Null convertirlos en 0 (cero)
- En elcampodistancia_ref si se encuentran campos en Null convertirlos en 0 (cero)

`transform_1.py`

```python
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
```
`transform_2.py`

```python
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
```

#### 5. Mostrar mediante una impresión de pantalla, que los tipos de campos de las tablas sean los solicitados en el datawarehouse (ej: fecha date, aeronave string, pasajeros integer, etc.).
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/5.png)

#### 6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query.

```sql
SELECT
	COUNT(1) AS vuelos
FROM aeropuerto_tabla
WHERE
	tipo_de_movimiento = 'Despegue'
	AND fecha BETWEEN '2021-12-01' AND '2022-01-31';
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/6.png)

#### 7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query.

```sql
SELECT
	SUM(pasajeros) AS pasajeros
FROM aeropuerto_tabla
WHERE 
	tipo_de_movimiento = 'Despegue'
	AND fecha BETWEEN '2021-01-01' AND '2022-06-30'
	AND aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA';
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/7.png)

#### 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y Resultado de la query.

```sql
SELECT 
	a.fecha, 
	a.horautc, 
	a.aeropuerto AS aeropuerto_salida,
   	adts.ref AS ciudad_salida, 
	a.origen_destino AS aeropuerto_arribo,
   	adta.ref AS ciudad_arribo, 
   	a.pasajeros
FROM aeropuerto_tabla a
INNER JOIN aeropuerto_detalles_tabla adts ON adts.aeropuerto = a.aeropuerto
INNER JOIN aeropuerto_detalles_tabla adta ON adta.aeropuerto = a.origen_destino
WHERE 
	a.tipo_de_movimiento = 'Despegue'
	AND a.fecha BETWEEN '2022-01-01' AND '2022-06-30'	 
ORDER BY a.fecha DESC;
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/8.png)

#### 9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización.

```sql
SELECT
	aerolinea_nombre,
	SUM(pasajeros) AS pasajeros
FROM aeropuerto_tabla
WHERE 
	tipo_de_movimiento = 'Despegue'
	AND fecha BETWEEN '2021-01-01' AND '2022-06-30'
	AND aerolinea_nombre != '0'
GROUP BY aerolinea_nombre
ORDER BY a.fecha DESC, a.horautc DESC;
LIMIT 10;
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/9.png)

#### 10. Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires, exceptuando aquellas aeronaves que no cuentan con nombre. Mostrar consulta y Visualización.

```sql
SELECT
	a.aeronave,
	COUNT(1) AS usos
FROM aeropuerto_tabla a
INNER JOIN aeropuerto_detalles_tabla ad ON ad.aeropuerto = a.aeropuerto
WHERE
	a.tipo_de_movimiento = 'Despegue'
	AND a.fecha BETWEEN '2021-01-01' AND '2022-06-30'
	AND a.aeronave != '0'
	AND ad.provincia LIKE '%BUENOS AIRES%'
GROUP BY a.aeronave
ORDER BY usos DESC
LIMIT 10;
```
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/10.png)

#### 11. Qué datos externos agregaría en este dataset que mejoraría el análisis de los datos.
Se podría incluir un número de vuelo que facilite la determinación de la consistencia entre despegues y aterrizajes. Además, sería útil incluir la matrícula para no limitarse únicamente al tipo de aeronave, lo que permitirá un análisis más preciso del rendimiento de cada avión en particular. Otro dato que generaría valor es incorporar un campo que refleje si el vuelo se realizó en tiempo estimado o no.

#### 12. Elabore sus conclusiones y recomendaciones sobre este proyecto.
El análisis del tráfico aéreo en Argentina indica que entre diciembre de 2021 y enero de 2022 hubo un gran aumento en los vuelos. Aerolíneas Argentinas fue la más destacada, seguida de JetSmart y Flybondi y los aviones más utilizados fueron los EMB-ERJ190100IGW. Para mejorar la experiencia de los pasajeros y hacer que las operaciones sean más eficientes, es importante actualizar y ampliar los aeropuertos más concurridos, mantener en buen estado las aeronaves más usadas para que estén disponibles el mayor tiempo posible, y apoyar el crecimiento de aerolíneas de bajo costo. Estas medidas ayudarán a que los aeropuertos funcionen mejor y permitirán que las aerolíneas respondan a la demanda.

#### 13. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Sí aplica).
![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_01/captures/diagram.png)
