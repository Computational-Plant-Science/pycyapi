#!/bin/bash

echo "Running local test job..."
LC_ALL=C.UTF-8 LANG=C.UTF-8 plantit --job "test_job_local.json"
echo "Running slurm test job..."
LC_ALL=C.UTF-8 LANG=C.UTF-8 plantit --job "test_job_slurm.json"
