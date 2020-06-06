#!/bin/bash

echo "Running integration tests..."
cluster --job "test_job.json" --run "local"
