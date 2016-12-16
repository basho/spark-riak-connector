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

from .riak_helper import helper
import pyspark
from pyspark.rdd import RDD

def riakBucket(self, bucket_name, bucket_type = "default"):
    return RiakKVRDD(self, bucket_name, bucket_type)

def saveToRiak(self, bucket_name, bucket_type = "default"):
    return helper(self.ctx).saveToRiak(self._jrdd, bucket_name, bucket_type)

class RiakKVRDD(RDD):

    def __init__(self, ctx, bucket_name, bucket_type):
        if not bucket_name:
            raise ValueError("bucket_name not set")

        if not bucket_type:
            self.bucket_type = "default"

        self.bucket_type = bucket_type
        self.bucket_name = bucket_name

        class DummyJRDD(object):
            def id(self):
                return -1
        jrdd = DummyJRDD()
        super(RiakKVRDD, self).__init__(jrdd, ctx)

    @property
    def _helper(self):
        return helper(self.ctx)

    def _pickle_jrdd(self):
        jrdd = self._helper.pickleRows(self._rrdd)
        return self._helper.javaRDD(jrdd)

    def get_rrdd(self):
        return self._crdd

    def set_rrdd(self, rrdd):
        self._rrdd = rrdd
        self._jrdd = self._pickle_jrdd()

    rrdd = property(get_rrdd, set_rrdd)

    saveToRiak = saveToRiak

    def queryAll(self):
        rdd_initial = self._helper.riakBucket(self.ctx._jsc, self.bucket_name, self.bucket_type)
        self.rrdd = rdd_initial.queryAll()
        return self
   
    def query2iKeys(self, index, *keys):
        self.rrdd = self._helper.query2iKeys(self.ctx._jsc, self.bucket_name, self.bucket_type, index, keys)
        return self

    def queryBucketKeys(self, *keys):
        self.rrdd = self._helper.queryBucketKeys(self.ctx._jsc, self.bucket_name, self.bucket_type, keys)
        return self
    
    def query2iRangeLocal(self, index, fromI, toI):
        rdd_initial = self._helper.riakBucket(self.ctx._jsc, self.bucket_name, self.bucket_type)
        self.rrdd = rdd_initial.query2iRangeLocal(index, fromI, toI)
        return self

    def query2iRange(self, index, fromI, toI):
        rdd_initial = self._helper.riakBucket(self.ctx._jsc, self.bucket_name, self.bucket_type)
        self.rrdd = rdd_initial.query2iRange(index, fromI, toI)
        return self

    def partitionBy2iRanges(self, index, *ranges):
        self.rrdd = self._helper.partitionBy2iRanges(self.ctx._jsc, self.bucket_name, self.bucket_type, index, ranges)
        return self

    def partitionBy2iKeys(self, index, *keys):
        self.rrdd = self._helper.partitionBy2iKeys(self.ctx._jsc, self.bucket_name, self.bucket_type, index, keys)
        return self
