#!/bin/bash

echo "Waiting for IRODS to start..."

./wait-for-postgres.sh irods
./wait-for-it.sh irods:1247 --

echo "Running unit tests..."

cd /opt/plantit-clusterside/ || exit
python3 -m pytest