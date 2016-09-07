#!/usr/bin/env bash

#
# Copyright (c) 2015 Basho Technologies, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# Spark Master URL
MASTER_URL="local"

# Riak connection host for spark-riak-connector
RIAK_HOST="127.0.0.1"

# Getting spark, scala and kafka versions
file="../../../../project/Versions.scala"

if [ -f "$file" ]
then
  while IFS=' = ' read -r key value
  do
    if [[ $value == spark* ]] || [[ $value == kafka* ]];
    then
        key=$(echo $key | tr '.' '_')
        IFS='= ' read -a myarray <<< "$value"
        if [[ $value == spark* ]]; then SPARK_VERSION=$(echo "${myarray[1]}" | tr -d \")
        else KAFKA_VERSION=$(echo "${myarray[1]}" | tr -d \")
        fi
    fi
  done < "$file"
else
  echo "$file not found."
fi

SCALA_VERSION=$(scala -version 2>&1 | grep -i version | awk '{ print substr($5, 0, 4); }')

# Uncomment this section to override these versions
# SPARK_VERSION="1.6.1"
# KAFKA_VERSION="0.8.2.2"
# SCALA_VERSION="2.10"

# Riak connection port for spark-riak-connector
RIAK_PORT="8087"

KAFKA_BROKER="127.0.0.1:9092"

DRIVER_MEM="512M"
EXECUTOR_MEM="512M"


SPARK_JARS=$(echo $BASEDIR/connector/target/scala-*/*-uber.jar | tr ' ' ',')
EXAMPLES_JARS=$(echo $BASEDIR/examples/target/scala-*/*.jar | tr ' ' ',')
