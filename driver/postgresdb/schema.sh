#!/usr/bin/env bash
psql -U postgres -h localhost <<EOF
CREATE USER apalia WITH PASSWORD 'apalia';
EOF

######################################
############ Cyclops DBs #############
######################################
psql -U postgres -h 160.85.31.84 <<EOF
CREATE DATABASE evaluation WITH OWNER apalia;
GRANT ALL PRIVILEGES ON DATABASE evaluation TO apalia;
<<EOF
\c evaluation
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EOF

######################################
########## Create Database ###########
######################################
psql -U apalia -h 160.85.31.84 -d evaluation <<EOF
CREATE TABLE IF NOT EXISTS evaluation (
  time            TIMESTAMP         NOT NULL,
  device_id       TEXT              NOT NULL,
  node_id         TEXT              NOT NULL,
  meter           TEXT		        NOT NULL,
  usage           DOUBLE PRECISION  NOT NULL
);
CREATE INDEX IF NOT EXISTS device_time ON evaluation (device_id, time DESC);
CREATE INDEX IF NOT EXISTS node_time ON evaluation (node_id, time DESC);
CREATE INDEX IF NOT EXISTS meter_time ON evaluation (meter, time DESC);
EOF


ALTER TABLE evaluation ALTER COLUMN time SET DEFAULT now();