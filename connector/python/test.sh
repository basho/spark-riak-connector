#!/usr/bin/env bash

set -e

export SPARK_CLASSPATH=$1

python connector/python/setup.py install && py.test -v -s -m $2 --junit-xml=connector/target/test-reports/com.basho.riak.pyspark.xml connector/python/tests/test_pyspark_riak.py
