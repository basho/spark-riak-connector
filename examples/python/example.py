import pyspark
import pyspark_riak

conf = pyspark.SparkConf().setAppName("My Spark Riak App")
conf.set("spark.riak.connection.host", "127.0.0.1:8087")
conf.set("spark.riak.connections.min", "20")
conf.set("spark.riak.connections.max", "50")
sc = pyspark.SparkContext(conf=conf)

# Enable Riak-specific APIs
pyspark_riak.riak_context(sc)

source_data = [{"key1":{"pr_key":"pr_val1"}}, {"key2":{"pr_key":"pr_val2"}}]
source_rdd = sc.parallelize(source_data, 1)
source_rdd.saveToRiak("test-python-bucket", "default")

rdd = sc.riakBucket("test-python-bucket", "default").queryAll()
data = rdd.collect() # will return list of Key-Value pairs
values = map(lambda x: x[1], data) # take values only
for e in values:
  print e['pr_key']
