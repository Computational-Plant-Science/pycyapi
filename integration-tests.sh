#!/bin/bash

echo "Waiting for IRODS to start..."

./wait-for-postgres.sh irods
./wait-for-it.sh irods:1247 --

echo "Running integration tests..."

iput /opt/plantit-clusterside/tests/sample1.jpg
iput /opt/plantit-clusterside/tests/sample2.jpg
iput /opt/plantit-clusterside/tests/sample3.jpg

python3 /opt/plantit-clusterside/tests/test.py
