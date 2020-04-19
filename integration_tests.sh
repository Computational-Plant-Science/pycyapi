#!/bin/sh

# bring up containers
docker-compose -f docker-compose.test.yml down
docker-compose -f docker-compose.test.yml up -d --build
./wait-for-postgres.sh localhost
./wait-for-it.sh localhost:1247 --
sleep 5 # for some reason IRODS exposes port 1247 before it's ready to accept incoming connections

# configure icommands
./configure-irods.sh

# set up test samples
iput tests/sample1.jpg
iput tests/sample2.jpg
iput tests/sample3.jpg

# run integration tests
python3 tests/test.py

# bring down containers
docker-compose -f docker-compose.test.yml down
