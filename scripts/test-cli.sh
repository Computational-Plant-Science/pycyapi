#!/bin/bash

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# params
plantit /examples/workflow_with_params.yaml
[ ! -f message.txt ] && exit 1
rm -r message.txt

# file input
# echo "Hello, world!" >> /test/input.txt
# plantit /examples/workflow_with_file_input.yaml --irods_host irods --irods_port 1247 --irods_username rods --irods_password rods --irods_zone tempZone
# [ ! -f input.output ] && exit 1
# rm -r input.output

# directory input
# plantit /examples/workflow_with_directory_input.yaml --irods_host irods --irods_port 1247 --irods_username rods --irods_password rods --irods_zone tempZone
# [ ! -f output.txt ] && exit 1
# rm -r output.txt

# file output
# plantit /examples/workflow_with_file_output.yaml --irods_host irods --irods_port 1247 --irods_username rods --irods_password rods --irods_zone tempZone
# [ ! -f output.txt ] && exit 1
# rm -r output.txt

# directory output
# plantit /examples/workflow_with_directory_output.yaml --irods_host irods --irods_port 1247 --irods_username rods --irods_password rods --irods_zone tempZone
# [ ! -f output.txt ] && exit 1
# rm -r output.txt