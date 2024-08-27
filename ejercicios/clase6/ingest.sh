#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL="https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.csv"
FILE_NAME=$(basename ${URL})

wget -P ${LANDING_DIR} ${URL}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/${FILE_NAME} ${INGEST_DIR}