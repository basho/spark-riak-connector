"""
Copyright 2016 Basho Technologies, Inc.
This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import pyspark
import types
from .riak_rdd import saveToRiak, riakBucket

def riak_context(context=None):
    if context==None:
        pyspark.context.SparkContext.riakBucket = riakBucket
        pyspark.rdd.RDD.saveToRiak = saveToRiak
    else:
        context.riakBucket = types.MethodType(riakBucket, context)
        pyspark.rdd.RDD.saveToRiak = saveToRiak
