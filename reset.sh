#!/usr/bin/env bash

docker rm -f $(docker ps -a -q) && docker system prune -f && \
docker compose -f ./docker-compose-rinha.yml up -d && \
docker compose -f ./docker-compose.yml up -d
