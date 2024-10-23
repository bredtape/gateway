#!/bin/sh

set -e

docker compose up --remove-orphans --build -d && docker compose logs -f
