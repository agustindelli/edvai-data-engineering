#!/bin/bash

hive -e "
CREATE DATABASE formula1;

USE formula1;

CREATE EXTERNAL TABLE IF NOT EXISTS driver_results (
    driver_forename STRING,
    driver_surname STRING,
    driver_nationality STRING,
    points FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/driver_results';

CREATE EXTERNAL TABLE IF NOT EXISTS constructor_results (
    constructorRef STRING,
    cons_name STRING,
    cons_nationality STRING,
    url STRING,
    points FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/constructor_results';
"