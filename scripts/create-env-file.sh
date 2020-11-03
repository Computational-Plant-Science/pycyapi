#!/bin/bash

if [[ -z "${CYVERSE_USERNAME}" ]]; then
  cyverse_username="some_cyverse_username"
  echo "Warning: CYVERSE_USERNAME environment variable missing"
else
  cyverse_username="${CYVERSE_USERNAME}"
fi

if [[ -z "${CYVERSE_PASSWORD}" ]]; then
  cyverse_password="some_cyverse_password"
  echo "Warning: CYVERSE_PASSWORD environment variable missing"
else
  cyverse_password="${CYVERSE_PASSWORD}"
fi

cat <<EOT >>".env"
CYVERSE_USERNAME=$cyverse_username
CYVERSE_PASSWORD=$cyverse_password
MYSQL_DATABASE=slurm_acct_db
MYSQL_USER=slurm
MYSQL_PASSWORD=password
EOT