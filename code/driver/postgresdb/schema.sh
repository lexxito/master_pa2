#!/usr/bin/env bash
psql -U postgres -h localhost <<EOF
CREATE USER mao_admin WITH PASSWORD 'splab';
EOF

######################################
############ Cyclops DBs #############
######################################
psql -U postgres -h localhost <<EOF
CREATE DATABASE mao WITH OWNER mao_admin;
GRANT ALL PRIVILEGES ON DATABASE mao TO mao_admin;
<<EOF
\c mao
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EOF

######################################
########## Create Database ###########
######################################
psql -U apalia -h localhost -d evaluation <<EOF
CREATE TABLE IF NOT EXISTS sources (
  time            TIMESTAMP         NOT NULL,
  name            TEXT              NOT NULL,
  metrics         JSONB             DEFAULT '{}'
  
);
CREATE INDEX IF NOT EXISTS device_time ON evaluation (name, time DESC);
EOF

