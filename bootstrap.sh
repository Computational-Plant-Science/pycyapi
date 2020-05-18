#!/bin/bash

echo "Bootstrapping..."

DOCKER_COMPOSE="docker-compose -f docker-compose.test.yml"

echo "Bringing containers down..."
$DOCKER_COMPOSE down

echo "Building containers..."
$DOCKER_COMPOSE build "$@" --no-cache

echo "Configuring mock IRODS..."
$DOCKER_COMPOSE up -d irods
$DOCKER_COMPOSE up -d cluster
$DOCKER_COMPOSE exec cluster /bin/bash /root/wait-for-it.sh irods:1247 -- /root/configure-irods.sh

echo "Stopping containers..."
$DOCKER_COMPOSE stop