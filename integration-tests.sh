#!/bin/bash

echo "Running integration tests..."
plantit --job "test_job.json" --run "local"
