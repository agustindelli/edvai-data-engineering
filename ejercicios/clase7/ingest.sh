#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL1="https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet"
FILE_NAME1=$(basename ${URL1})

URL2="https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-02.parquet"
FILE_NAME2=$(basename ${URL2})

wget -P ${LANDING_DIR} ${URL1} ${URL2}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/${FILE_NAME1} ${INGEST_DIR}
${HADOOP_BIN} dfs -put ${LANDING_DIR}/${FILE_NAME2} ${INGEST_DIR}