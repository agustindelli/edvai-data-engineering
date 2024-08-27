#!/bin/bash

# 1. Mostrar las tablas de la base de datos northwind
sqoop list-tables \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password edvai

# 2. Mostrar los clientes de Argentina 
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password edvai
--query "select * from customers where country = 'Argentina'"

# 3. Importar un archivo .parquet que contenga toda la tabla orders. Luego ingestar el archivo a HDFS (carpeta /sqoop/ingest)   
sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--table orders \
--m 1 \
--password edvai
--target-dir home/hadoop/sqoop/ingest \
--as-parquetfile \
--delete-target-dir

# 4. Importar un archivo .parquet que contenga solo los productos con mas 20 unidades en stock, de la tabla Products . Luego ingestar el archivo a HDFS (carpeta ingest) 
sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--table products \
--m 1 \
--password edvai
--target-dir home/hadoop/sqoop/ingest \
--as-parquetfile \
--where "units_in_stock > 20" \
--delete-target-dir