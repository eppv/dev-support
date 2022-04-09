#!/usr/bin/env bash

docker kill $(docker ps -q) &&
yes | docker container prune &&
yes | docker volume prune &&
printf "Docker containers and volumes purged\n"
