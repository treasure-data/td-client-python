#!/bin/bash
set -euo pipefail

echo "${TD_API_ENDPOINT}"
echo "${TD_API_KEY}"

RESPONSE=$(td -e "${TD_API_ENDPOINT}" -k "${TD_API_KEY}" workflow start aki_pytd_test aki_pytd_test --session now)
SESSION_ID=$(echo "${RESPONSE}" | sed -nE 's/^[ \t]*session id:[ \t]+([0-9]+)$/\1/p')

if [ -z "${SESSION_ID}" ]; then
  echo "Failed to start workflow session: ${SESSION_ID}"
  exit 1
else
  echo "Started workflow session with ID: ${SESSION_ID}"
fi

until false
do
  STATUS=$(td -e "${TD_API_ENDPOINT}" -k "${TD_API_KEY}" workflow session ${SESSION_ID} | sed -nE 's/[ \t]*status:[ \t]+(.*)/\1/p')
  if [ "${STATUS}" = "success" ]; then
    echo "Workflow completed successfully."
    exit 0
  elif [ "${STATUS}" = "error" ]; then
    echo "Workflow failed."
    exit 1
  elif [ "${STATUS}" = "running" ]; then
    echo "Workflow running. Waiting for completion..."
    sleep 10
  else
    echo "Unknown status: ${STATUS}. Exiting."
    exit 1
  fi
done
