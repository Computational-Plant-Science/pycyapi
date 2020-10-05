#!/bin/bash

if [[ -z "${CYVERSE_USERNAME}" ]]; then
  github_secret="some_cyverse_username"
  echo "Warning: CYVERSE_USERNAME environment variable missing"
else
  github_secret="${CYVERSE_USERNAME}"
fi

if [[ -z "${CYVERSE_PASSWORD}" ]]; then
  cas_server_url="some_cyverse_password"
  echo "Warning: CYVERSE_PASSWORD environment variable missing"
else
  cas_server_url="${CYVERSE_PASSWORD}"
fi

cat <<EOT >>".env"
CYVERSE_USERNAME=
CYVERSE_PASSWORD
EOT