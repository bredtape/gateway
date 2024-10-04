#!/bin/sh

set -e

docker compose down --volumes
exec docker compose up --remove-orphans
