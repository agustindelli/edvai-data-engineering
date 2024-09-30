#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL1="https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv"
URL2="https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"

rm -f ${LANDING_DIR}/*

wget -P ${LANDING_DIR} ${URL1} ${URL2}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/* ${INGEST_DIR}

rm -f ${LANDING_DIR}/*