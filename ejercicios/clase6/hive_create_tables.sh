#!/bin/bash

hive -e "
USE tripdata;

CREATE TABLE IF NOT EXISTS payments (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    payment_type INT,
    total_amount FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS passengers (
    tpep_pickup_datetime TIMESTAMP,
    passenger_count INT,
    total_amount FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS tolls (
    tpep_pickup_datetime TIMESTAMP,
    passenger_count INT,
    tolls_amount FLOAT,
    total_amount FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS congestion (
    tpep_pickup_datetime TIMESTAMP,
    passenger_count INT,
    congestion_surcharge FLOAT,
    total_amount FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS distance (
    tpep_pickup_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    total_amount FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
"