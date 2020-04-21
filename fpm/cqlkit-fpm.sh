#!/bin/bash

cd /data
for i in rpm deb; do
  echo $i
  fpm -s dir -t $i -a all -n "cqlkit" -v "${VERSION}" \
  -m study@tenmax.io \
  --vendor TenMax \
  --license Apache \
  --description "CLI tool to export Cassandra query as CSV and JSON format.
  cqlkit is a CLI tool to export Cassandra query to CSV and JSON format.
  Cassandra is not good at Ad-hoc query, cqlkit allows you to export query
  result to semi-structured(JSON) or structured data(CSV).
  There are many tools out there for you to query or process these kinds of format.

  For more details, please check https://github.com/tenmax/cqlkit.
  " \
  --url https://github.com/tenmax/cqlkit \
  "/data/cqlkit/=/usr/share/cqlkit" \
  /data/fpm/bin/=/usr/bin/
done
