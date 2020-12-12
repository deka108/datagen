#!/usr/bin/env bash
# CMD: [Dir to TPCH-sqlite]
# git clone git@github.com:lovasoa/TPCH-sqlite.git

usage="Usage: $0 [PATH TO TPCH-sqlite] [SCALE]"
: ${1?$usage}
: ${2?$usage}

path_to_repo=$1
scale=$2

set -x
ori_path=`pwd`
cd $path_to_repo && \
  rm -rf tpch-dbgen/dbgen && \
  make SCALE_FACTOR="$scale"
cd $ori_path && \
  mv "$path_to_repo/TPC-H.db" tpch
set +x

./count.sh $(pwd)/tpch/TPC-H.db
