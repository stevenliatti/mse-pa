#!/usr/bin/env bash

docker exec -it ksqldb-cli ksql --config-file /ksql-init.conf -- http://ksqldb-server:8088
