#!/usr/bin/env bash

# Spark Master URL
MASTER_URL="spark://ip-172-31-9-126:7077"

# Riak connection host for spark-riak-connector
RIAK_HOST="172.31.9.126"

# Riak connection port for spark-riak-connector
RIAK_PORT="10017"

DRIVER_MEM="512M"
EXECUTOR_MEM="512M"
MAIN_CLASS="com.basho.spark.connector.demos.ofac.OFACDemo"
