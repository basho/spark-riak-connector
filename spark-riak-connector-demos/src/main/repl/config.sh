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
MASTER_URL="spark://ip-172-31-9-126:7077"

# Riak connection host for spark-riak-connector
RIAK_HOST="172.31.9.126"

# Riak connection port for spark-riak-connector
RIAK_PORT="10017"

DRIVER_MEM="512M"
EXECUTOR_MEM="512M"
MAIN_CLASS="com.basho.spark.connector.demos.ofac.OFACDemo"
