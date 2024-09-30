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