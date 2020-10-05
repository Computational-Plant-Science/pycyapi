#!/bin/bash

echo "Bootstrapping ${PWD##*/} development environment..."
compose="docker-compose -f docker-compose.test.yml"
nocache=0

while getopts 'n' opt; do
    case $opt in
        n) nocache=1 ;;
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
  echo "Building containers..."
  $compose build "$@"
else
  echo "Building containers with option '--no-cache'..."
  $compose build "$@" --no-cache
fi

echo "Configuring mock IRODS..."
$compose up -d cluster

echo "Stopping containers..."
$compose stop