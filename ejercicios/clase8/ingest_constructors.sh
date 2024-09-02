#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL="https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/constructors.csv"
FILE_NAME=$(basename ${URL})

wget -P ${LANDING_DIR} ${URL}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/${FILE_NAME} ${INGEST_DIR}

rm -f ${LANDING_DIR}/${FILE_NAME}