#!/bin/bash

hive -e "
USE formula1;

DESCRIBE driver_results;
DESCRIBE constructor_results;
"