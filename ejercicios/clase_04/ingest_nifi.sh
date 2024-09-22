#!/bin/bash

INGEST_DIR="/home/nifi/ingest"
URL="https://dataengineerpublic.blob.core.windows.net/data-engineer/starwars.csv"

wget -P ${INGEST_DIR} ${URL}
