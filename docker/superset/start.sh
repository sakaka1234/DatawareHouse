#!/bin/bash
set -e

superset db upgrade
superset fab create-admin \
  --username admin --password admin --firstname Admin --lastname User \
  --email admin@example.com || true
superset init
exec /usr/bin/run-server.sh
