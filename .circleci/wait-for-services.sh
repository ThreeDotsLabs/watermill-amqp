#!/bin/bash
set -e

for service in rabbitmq:5672; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
