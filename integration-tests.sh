#!/bin/bash

echo "Running local integration test..."
LC_ALL=C.UTF-8 LANG=C.UTF-8 plantit --job "test_job.json" --run "local"
echo "Running jobqueue integration test..."
LC_ALL=C.UTF-8 LANG=C.UTF-8 plantit --job "test_job.json" --run "jobqueue"
