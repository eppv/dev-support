#!/usr/bin/env bash

# region var

data_dir="$HOME/data/dwh"

# endregion

if [ -d "$data_dir" ]
then
  printf "Host data directory exists.\n"
else
  printf "Creating data directory for volume mount..."
  mkdir -pv "$data_dir"
fi &&

docker run \
--name pg_dwh \
-p 5432:5432 \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=pgpwd \
-e POSTGRES_DB=postgres \
-e PGDATA=/var/lib/postgresql/data/pgdata \
-d \
-v /var/run/postgresql/postgres.sock:/var/run/postgresql/postgres.sock \
-v "$data_dir":/var/lib/postgresql/data \
postgres:14.2-alpine3.15