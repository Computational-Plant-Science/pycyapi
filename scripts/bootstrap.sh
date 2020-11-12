#!/bin/bash

echo "Bootstrapping ${PWD##*/} test environment..."
compose="docker-compose -f docker-compose.test.yml"
nocache=0
quiet=0

while getopts 'nq' opt; do
    case $opt in
        n) nocache=1 ;;
        q) quiet=1 ;;
        *) echo 'Error in command line parsing' >&2
           exit 1
    esac
done
shift "$(( OPTIND - 1 ))"

echo "Bringing containers down..."
$compose down

env_file=".env"
echo "Checking for environment variable file '$env_file'..."
if [ ! -f $env_file ]; then
  echo "Environment variable file '$env_file' does not exist. Creating it..."
  chmod +x scripts/create-env-file.sh
  ./scripts/create-env-file.sh
else
  echo "Environment variable file '$env_file' already exists. Continuing..."
fi

if [[ "$nocache" -eq 0 ]]; then
  if [[ "$quiet" -eq 0 ]]; then
    echo "Building containers..."
    docker build -t computationalplantscience/plantit-sandbox -f dockerfiles/sandbox/Dockerfile .
  else
    echo "Building containers quietly..."
    docker build -t computationalplantscience/plantit-sandbox -q -f dockerfiles/sandbox/Dockerfile .
  fi
else
  if [[ "$quiet" -eq 0 ]]; then
    echo "Building containers with cache disabled..."
    docker build -t computationalplantscience/plantit-sandbox --no-cache -f dockerfiles/sandbox/Dockerfile .
  else
    echo "Building containers quietly with cache disabled..."
    docker build -t computationalplantscience/plantit-sandbox -q --no-cache -f dockerfiles/sandbox/Dockerfile .
  fi
fi

echo "Pulling 3rd-party images and bringing containers up..."
$compose up -d --quiet-pull