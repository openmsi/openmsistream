#!/bin/bash

set -euxo pipefail

docker compose -f local-kafka-broker-docker-compose.yml down -t 0
