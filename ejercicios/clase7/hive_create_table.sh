#!/bin/bash

hive -e "
USE tripdata;

CREATE EXTERNAL TABLE IF NOT EXISTS airport_trips (
    tpep_pickup_datetime TIMESTAMP,
    airport_fee FLOAT,
    payment_type INT,
    tolls_amount FLOAT,
    total_amount FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/airport_trips';
"