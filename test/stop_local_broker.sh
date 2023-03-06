#!/bin/sh

set -euxo pipefail

docker compose -f local-kafka-broker-docker-compose.yml down 