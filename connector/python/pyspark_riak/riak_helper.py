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

_helper = None
def helper(ctx):
    global _helper

    if not _helper:
        _helper = load_class(ctx, "com.basho.riak.spark.util.python.RiakPythonHelper").newInstance()

    return _helper

''' 
    After migrating to Spark 2.0 shorter notation should be available 
    java_import(self._gateway.jvm, "com.basho.riak.spark.japi.SparkJavaUtil")
    scf = self._gateway.jvm.SparkJavaUtil.javaFunctions(self)
	(see comments https://bashoeng.atlassian.net/browse/SPARK-122)
'''
def load_class(ctx, name):
    return ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(name)


