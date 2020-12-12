#!/usr/bin/env bash

usage="Usage: $0 [PATH TO SQLITE db]"
: ${1?$usage}
path_to_db=$1

# get count of all tables
for name in `sqlite3 $path_to_db "SELECT name FROM sqlite_master WHERE type='table';"`; do
  sqlite3 $path_to_db "SELECT COUNT(*) AS count, '$name' AS name FROM $name;";
done