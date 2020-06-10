#!/bin/bash

nocache=0

while getopts 'n' opt; do
    case $opt in
        n) nocache=1 ;;
        *) echo 'Error in command line parsing' >&2
           exit 1
    esac
done
shift "$(( OPTIND - 1 ))"

echo "Bootstrapping ${PWD##*/}..."

DOCKER_COMPOSE="docker-compose -f docker-compose.test.yml"

echo "Bringing containers down..."
$DOCKER_COMPOSE down

if [[ "$nocache" -eq 0 ]]; then
  echo "Building containers..."
  $DOCKER_COMPOSE build "$@"
else
  echo "Building containers with option '--no-cache'..."
  $DOCKER_COMPOSE build "$@" --no-cache
fi

echo "Configuring mock IRODS..."
$DOCKER_COMPOSE up -d irods
$DOCKER_COMPOSE up -d cluster
$DOCKER_COMPOSE exec cluster /bin/bash /root/wait-for-it.sh irods:1247 -- /root/configure-irods.sh

echo "Stopping containers..."
$DOCKER_COMPOSE stop