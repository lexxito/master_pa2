#!/usr/bin/env bash
influx <<EOF
CREATE USER apalia WITH PASSWORD 'apalia' WITH ALL PRIVILEGES;
EOF
