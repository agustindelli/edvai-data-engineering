#!/bin/bash

hive -e "
USE tripdata;

DESCRIBE passengers;
DESCRIBE distance;
"