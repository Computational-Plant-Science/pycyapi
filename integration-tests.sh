#!/bin/bash

# echo "Waiting for IRODS to start..."
# ./wait-for-postgres.sh irods
# ./wait-for-it.sh irods:1247 --

echo "Running integration tests..."
clusterside --job "test_job.json" --executor "local"
