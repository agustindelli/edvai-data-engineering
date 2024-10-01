#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL1="https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv"
URL2="https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv"
URL3="https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv"

rm -f ${LANDING_DIR}/*

wget -P ${LANDING_DIR} ${URL1} ${URL2} ${URL3}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/* ${INGEST_DIR}

rm -f ${LANDING_DIR}/*