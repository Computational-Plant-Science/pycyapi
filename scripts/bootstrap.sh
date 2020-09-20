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