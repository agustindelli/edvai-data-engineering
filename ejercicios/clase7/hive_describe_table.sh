#!/bin/bash

hive -e "
USE tripdata;

DESCRIBE airport_trips;
"