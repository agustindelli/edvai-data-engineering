#!/bin/bash

LANDING_DIR="/home/hadoop/landing"
INGEST_DIR="/ingest"

HADOOP_BIN="/home/hadoop/hadoop/bin/hdfs"

URL1="https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/results.csv"
URL2="https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/drivers.csv"
URL3="https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/constructors.csv"
URL4="https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/races.csv"

rm -f ${LANDING_DIR}/*

wget -P ${LANDING_DIR} ${URL1} ${URL2} ${URL3} ${URL4}

${HADOOP_BIN} dfs -put ${LANDING_DIR}/* ${INGEST_DIR}

rm -f ${LANDING_DIR}/*