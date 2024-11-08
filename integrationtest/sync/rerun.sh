#!/bin/sh

set -e

docker compose up --remove-orphans --build -d && docker compose logs -f sync_a sync_b sync_c
