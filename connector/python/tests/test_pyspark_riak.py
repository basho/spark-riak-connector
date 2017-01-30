from __future__ import print_function
import pytest
import sys
from operator import add
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext, Row
import os, subprocess, json, riak, time
import pyspark_riak
import timeout_decorator
import datetime
import tzlocal
import pytz
import math
from pyspark_tests_fixtures import *
from random import randint

#### Notes ####
'''
Saving ints to riak ts preserves the value of the timestamp.
Querying ints using riak client is, in this case, simple, just query the int range

Saving datetimes to riak ts, the datetimes will be treated as local time, converted then to gmt time.
You can query with riak client by int only, so in this case you must convert your local datetime to utc int.
If you do ts_get, you can use local datetime to query. The query will be converted automatically to utc before query.

Reading datetime from ts using spark timestamp option will convert datetime back to local datetime.
'''
###### FUNCTIONS #######

def setup_table(client):

    riak_ts_table_name = 'spark-riak-%d' % int(time.time())
    riak_ts_table = client.table(riak_ts_table_name)

    create_sql = """CREATE TABLE %(table_name)s (
    field1 varchar not null,
    field2 varchar not null,
    datetime timestamp not null,
    data sint64,
    PRIMARY KEY ((field1, field2, quantum(datetime, 24, h)), field1, field2, datetime))
    """ % ({'table_name': riak_ts_table_name})

    return riak_ts_table_name, create_sql, riak_ts_table

def setup_kv_obj(client, bucket_name, key, content_type, data):

    bucket = client.bucket(bucket_name)
    obj = riak.RiakObject(client, bucket, key)
    obj.content_type = content_type
    obj.data = data
    return obj

def setup_ts_obj(ts_table, data):

    return ts_table.new(data)

def unix_time_seconds(dt):
    td = dt - datetime.datetime.utcfromtimestamp(0)
    return int(td.total_seconds())

def unix_time_millis(dt):
    td = unix_time_seconds(dt)
    return int(td * 1000.0)

def make_data_long(start_date, N, M):

    data = []
    one_second = datetime.timedelta(seconds=1)
    one_day = datetime.timedelta(days=1)

    for i in range(M):
        for j in range(N):

            data.append(['field1_val', 'field2_val', unix_time_millis(start_date + i*one_day + j*one_second), i+j])


    end_date = start_date + (M-1)*one_day + (N-1)*one_second
    return data, start_date, end_date

def make_data_timestamp(start_date, N, M):

    timestamp_data = []
    long_data = []

    one_second = datetime.timedelta(seconds=1)
    one_day = datetime.timedelta(days=1)
    local_start_date = convert_to_local_dt(start_date)

    for i in range(M):
        for j in range(N):

            cur_local_timestamp = local_start_date + i*one_day + j*one_second
            timestamp_data.append(['field1_val', 'field2_val', cur_local_timestamp, i+j])
            long_data.append(['field1_val', 'field2_val', unix_time_millis(convert_dt_to_gmt_dt(cur_local_timestamp)), i+j])

    start_timestamp = convert_dt_to_gmt_dt(timestamp_data[0][2])
    end_timestamp = convert_dt_to_gmt_dt(timestamp_data[-1][2])

    start_long = long_data[0][2]
    end_long = long_data[-1][2]

    return timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long
def convert_dt_to_gmt_dt(dt):

    gmt_dt_with_tzinfo = pytz.utc.normalize(dt)

    year = gmt_dt_with_tzinfo.year
    month = gmt_dt_with_tzinfo.month
    day = gmt_dt_with_tzinfo.day
    hour = gmt_dt_with_tzinfo.hour
    minute = gmt_dt_with_tzinfo.minute
    second = gmt_dt_with_tzinfo.second

    gmt_dt = datetime.datetime(year, month, day, hour, minute, second)

    return gmt_dt

def convert_to_local_dt(dt):

#    local_tz = tzlocal.get_localzone()
    local_tz = pytz.utc
    local_dt = local_tz.localize(dt)
    return local_dt

def convert_local_dt_to_gmt_dt(dt):
    local_dt = convert_to_local_dt(dt)
    return convert_dt_to_gmt_dt(local_dt)

def make_table_with_data(N, M, useLong, spark_context, riak_client):

	riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

	riak_ts_table.query(create_sql)

	seed_date = datetime.datetime(2016, 1, 1, 12, 0, 0)

	if useLong:

		test_data, start, end = make_data_long(seed_date, N, M)
		test_rdd = spark_context.parallelize(test_data)

	else:

		timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long = make_data_timestamp(seed_date, N, M)
		test_rdd = spark_context.parallelize(timestamp_data)

	test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])

	test_df.write.format('org.apache.spark.sql.riak').mode('Append').save(riak_ts_table_name)

	if useLong:

		return start, end, riak_ts_table_name, test_df, test_rdd, test_data, riak_ts_table

	else:

		return start_timestamp, end_timestamp, riak_ts_table_name, test_df, test_rdd, timestamp_data, long_data, start_long, end_long, riak_ts_table

def make_kv_data(N, spark_context):

	source_data = []
	test_data = []
	keys = []
	bad_keys = []

	for i in range(N):
		keys.append(str(u'key'+str(i)))
		source_data.append({str(u'key'+str(i)) : {u'data' : i}})
		test_data.append( (str(u'key'+str(i)),{u'data' : i}))
		bad_keys.append(str(i))

	source_rdd = spark_context.parallelize(source_data)

	return source_rdd, source_data, test_data, keys, bad_keys

def make_kv_data_2i(N, test_bucket_name, riak_client):

	bucket = riak_client.bucket_type('default').bucket(test_bucket_name)
	test_data = []
	string2i = []
	integer2i = []
	partitions = []
	bad_partitions = []

	for i in range(N):

		obj = riak.RiakObject(riak_client, bucket, str(u'key'+str(i)))
		obj.content_type = 'application/json'
		obj.data = {u'data' : i}
		obj.add_index('string_index_bin', 'string_val_'+str(i))
		obj.add_index('integer_index_int', i)

		obj.store()

		test_data.append((str('key'+str(i)),{u'data' : i}))
		string2i.append('string_val_'+str(i))
		integer2i.append(i)
		partitions.append((i,i))
		bad_partitions.append((N+i,N+i))

	return test_data, string2i, integer2i, partitions, bad_partitions

def make_filter(useLong, start, end):

	if useLong:

		temp_filter = """datetime >= %(start_date)s
		AND datetime <=  %(end_date)s
		AND field1 = '%(field1)s'
		AND field2 = '%(field2)s'
		""" % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})
	else:

		temp_filter = """datetime >= CAST(%(start_date)s AS TIMESTAMP)
		AND datetime <=  CAST(%(end_date)s AS TIMESTAMP)
		AND field1 = '%(field1)s'
		AND field2 = '%(field2)s'
		""" % ({'start_date': start, 'end_date': end, 'field1': 'field1_val', 'field2': 'field2_val'})

	return temp_filter

def make_ts_query(riak_ts_table_name, start, end):

	fmt =   """
	select * from {table_name}
	where datetime >= {start_date}
	AND datetime <=  {end_date}
	AND field1 = '{field1}'
	AND field2 = '{field2}'
	"""
	query = fmt.format(table_name=riak_ts_table_name, start_date=unix_time_millis(start), end_date=unix_time_millis(end), field1='field1_val', field2='field2_val')
	return query

###### TESTS #######

# def _test_connection(spark_context, riak_client, sql_context):
#
#     riak_client.ping()
#
#     obj = setup_kv_obj(riak_client, 'temp_bucket', 'temp_key', 'text/plain', 'temp_data')
#
#     obj.store()
#
#     result = riak_client.bucket('temp_bucket').get('temp_key')
#
#     assert result.data == 'temp_data'
#
#     riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)
#
#     riak_ts_table.query(create_sql)
#
#     time.sleep(5)
#
#     ts_obj = setup_ts_obj(riak_ts_table, [['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0)), 0]])
#
#     ts_obj.store()
#
#     result = riak_client.ts_get(riak_ts_table_name, ['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0))])
#
#     assert result.rows == [['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0)), 0]]

###### Riak TS Test #######

def _test_spark_df_ts_write_use_long(N, M, spark_context, riak_client, sql_context):

	useLong=True
	start, end, riak_ts_table_name, test_df, test_rdd, test_data, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)
	query = make_ts_query(riak_ts_table_name, start, end)

	result = riak_ts_table.query(query)

	assert sorted(result.rows, key=lambda x: x[2]) == sorted(test_rdd.collect(), key=lambda x: x[2])

def _test_spark_df_ts_write_use_timestamp(N, M, spark_context, riak_client, sql_context):

	useLong=False
	start_timestamp, end_timestamp, riak_ts_table_name, test_df, test_rdd, timestamp_data, long_data, start_long, end_long, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)

	query = make_ts_query(riak_ts_table_name, start_timestamp, end_timestamp)

	result = riak_ts_table.query(query)
	assert sorted(result.rows, key=lambda x: x[2]) == sorted(spark_context.parallelize(long_data).collect(), key=lambda x: x[2])

def _test_spark_df_ts_read_use_long(N, M, spark_context, riak_client, sql_context):

	useLong=True
	start, end, riak_ts_table_name, test_df, test_rdd, test_data, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)

	temp_filter = make_filter(useLong, start, end)

	result = sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useLong").load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) == sorted(test_df.collect(), key=lambda x: x[2])

def _test_spark_df_ts_read_use_long_ts_quantum(N, M, spark_context, riak_client, sql_context):

	useLong=True
	start, end, riak_ts_table_name, test_df, test_rdd, test_data, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)
	temp_filter = make_filter(useLong, start, end)

	result = sql_context.read.format("org.apache.spark.sql.riak") \
	.option("spark.riakts.bindings.timestamp", "useLong") \
	.option("spark.riak.partitioning.ts-quantum", "24h") \
	.load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) == sorted(test_df.collect(), key=lambda x: x[2])

def _test_spark_df_ts_read_use_timestamp(N, M, spark_context, riak_client, sql_context):

	useLong=False
	start_timestamp, end_timestamp, riak_ts_table_name, test_df, test_rdd, timestamp_data, long_data, start_long, end_long, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)
	temp_filter = make_filter(useLong, unix_time_seconds(start_timestamp), unix_time_seconds(end_timestamp))

	result = sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useTimestamp").load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) == sorted(test_df.collect(), key=lambda x: x[2])

def _test_spark_df_ts_read_use_timestamp_ts_quantum(N, M, spark_context, riak_client, sql_context):

	useLong=False
	start_timestamp, end_timestamp, riak_ts_table_name, test_df, test_rdd, timestamp_data, long_data, start_long, end_long, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)

	temp_filter = make_filter(useLong, unix_time_seconds(start_timestamp), unix_time_seconds(end_timestamp))

	result = sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useTimestamp").option("spark.riak.partitioning.ts-quantum", "24h").load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) == sorted(test_df.collect(), key=lambda x: x[2])

def _test_spark_df_ts_range_query_input_split_count_use_long(N, M, S,spark_context, riak_client, sql_context):

	useLong=True
	start, end, riak_ts_table_name, test_df, test_rdd, test_data, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)
	time.sleep(1)
	temp_filter = make_filter(useLong, start, end)

	result = sql_context.read.format("org.apache.spark.sql.riak") \
						.option("spark.riakts.bindings.timestamp", "useLong") \
						.option("spark.riak.input.split.count", str(S)) \
						.option("spark.riak.partitioning.ts-range-field-name", "datetime") \
						.load(riak_ts_table_name).filter(temp_filter)


	assert sorted(result.collect(), key=lambda x: x[2]) ==  sorted(test_df.collect(), key=lambda x: x[2])
	assert result.rdd.getNumPartitions() ==  S

def _test_spark_df_ts_range_query_input_split_count_use_long_ts_quantum(N, M, S,spark_context, riak_client, sql_context):

	useLong=True
	start, end, riak_ts_table_name, test_df, test_rdd, test_data, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)
	temp_filter = make_filter(useLong, start, end)

	result = sql_context.read.format("org.apache.spark.sql.riak") \
	.option("spark.riakts.bindings.timestamp", "useLong") \
	.option("spark.riak.partitioning.ts-quantum", "24h") \
	.option("spark.riak.input.split.count", str(S)) \
	.option("spark.riak.partitioning.ts-range-field-name", "datetime") \
	.load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) ==  sorted(test_df.collect(), key=lambda x: x[2])
	assert result.rdd.getNumPartitions() ==  S

def _test_spark_df_ts_range_query_input_split_count_use_timestamp(N, M, S,spark_context, riak_client, sql_context):

	useLong=False
	start_timestamp, end_timestamp, riak_ts_table_name, test_df, test_rdd, timestamp_data, long_data, start_long, end_long, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)

	temp_filter = make_filter(useLong, unix_time_seconds(start_timestamp), unix_time_seconds(end_timestamp))
	result = sql_context.read.format("org.apache.spark.sql.riak") \
	.option("spark.riakts.bindings.timestamp", "useTimestamp") \
	.option("spark.riak.input.split.count", str(S)) \
	.option("spark.riak.partitioning.ts-range-field-name", "datetime") \
	.load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) ==  sorted(test_df.collect(), key=lambda x: x[2])
	assert result.rdd.getNumPartitions() ==  S

def _test_spark_df_ts_range_query_input_split_count_use_timestamp_ts_quantum(N, M, S,spark_context, riak_client, sql_context):

	useLong=False
	start_timestamp, end_timestamp, riak_ts_table_name, test_df, test_rdd, timestamp_data, long_data, start_long, end_long, riak_ts_table = make_table_with_data(N, M, useLong, spark_context, riak_client)
	temp_filter = make_filter(useLong, unix_time_seconds(start_timestamp), unix_time_seconds(end_timestamp))
	result = sql_context.read.format("org.apache.spark.sql.riak") \
	.option("spark.riakts.bindings.timestamp", "useTimestamp") \
	.option("spark.riak.partitioning.ts-quantum", "24h") \
	.option("spark.riak.input.split.count", str(S)) \
	.option("spark.riak.partitioning.ts-range-field-name", "datetime") \
	.load(riak_ts_table_name).filter(temp_filter)

	assert sorted(result.collect(), key=lambda x: x[2]) ==  sorted(test_df.collect(), key=lambda x: x[2])
	assert result.rdd.getNumPartitions() ==  S

###### Riak KV Tests ######

def _test_spark_rdd_write_kv(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	source_rdd, source_data, test_data, keys, bad_keys = make_kv_data(N, spark_context)

	source_rdd.saveToRiak(test_bucket_name, "default")

	test_data = [{x.key: x.data} for x in riak_client.bucket(test_bucket_name).multiget(keys)]

	assert sorted(source_data) == sorted(test_data)

def _test_spark_rdd_kv_read_query_all(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	source_rdd, source_data, test_data, keys, bad_keys = make_kv_data(N, spark_context)

	source_rdd.saveToRiak(test_bucket_name, "default")

	result = spark_context.riakBucket(test_bucket_name).queryAll()

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted(test_data, key=lambda x: x[0])

def _test_spark_rdd_kv_read_query_bucket_keys(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	source_rdd, source_data, test_data, keys, bad_keys = make_kv_data(N, spark_context)

	source_rdd.saveToRiak(test_bucket_name)

	result = spark_context.riakBucket(test_bucket_name).queryBucketKeys(*keys)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted(test_data, key=lambda x: x[0])

	result = spark_context.riakBucket(test_bucket_name).queryBucketKeys(*bad_keys)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted([], key=lambda x: x[0])

def _test_spark_rdd_kv_read_query_2i_keys(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	test_data, string2i, integer2i, partitions, bad_partitions = make_kv_data_2i(N, test_bucket_name, riak_client)

	result = spark_context.riakBucket(test_bucket_name).query2iKeys('string_index', *string2i)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted(test_data, key=lambda x: x[0])

	result = spark_context.riakBucket(test_bucket_name).query2iKeys('integer_index', *integer2i)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted(test_data, key=lambda x: x[0])

def _test_spark_rdd_kv_read_query2iRange(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	test_data, string2i, integer2i, partitions, bad_partitions = make_kv_data_2i(N, test_bucket_name, riak_client)

	result = spark_context.riakBucket(test_bucket_name).query2iRange('integer_index', integer2i[0], integer2i[-1])

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted(test_data, key=lambda x: x[0])

	result = spark_context.riakBucket(test_bucket_name).query2iRange('integer_index', N, 2*N)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted([], key=lambda x: x[0])

def _test_spark_rdd_kv_read_partition_by_2i_range(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	test_data, string2i, integer2i, partitions, bad_partitions = make_kv_data_2i(N, test_bucket_name, riak_client)

	result = spark_context.riakBucket(test_bucket_name).partitionBy2iRanges('integer_index', *partitions)

	assert sorted(result.collect(), key=lambda x: x[0]), sorted(test_data, key=lambda x: x[0])

	assert result.getNumPartitions() == N

	result = spark_context.riakBucket(test_bucket_name).partitionBy2iRanges('integer_index', *bad_partitions)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted([], key=lambda x: x[0])

	assert result.getNumPartitions() == N

def _test_spark_rdd_kv_read_partition_by_2i_keys(N, spark_context, riak_client, sql_context):

	test_bucket_name = "test-bucket-"+str(randint(0,100000))

	test_data, string2i, integer2i, partitions, bad_partitions = make_kv_data_2i(N, test_bucket_name, riak_client)

	result = spark_context.riakBucket(test_bucket_name).partitionBy2iKeys('string_index', *string2i)

	assert sorted(result.collect(), key=lambda x: x[0]), sorted(test_data, key=lambda x: x[0])

	assert result.getNumPartitions() == N

	bad_strings = ['no', 'nein', 'net']

	result = spark_context.riakBucket(test_bucket_name).partitionBy2iKeys('string_index', *bad_strings)

	assert sorted(result.collect(), key=lambda x: x[0]) == sorted([], key=lambda x: x[0])

	assert result.getNumPartitions() == len(bad_strings)

###### Run Tests ######

# def test_con(spark_context, riak_client, sql_context):
# 	_test_connection(spark_context, riak_client, sql_context)

###### KV Tests #######

@pytest.mark.riakkv
def test_kv_write(spark_context, riak_client, sql_context):
	_test_spark_rdd_write_kv(10, spark_context, riak_client, sql_context)

@pytest.mark.riakkv
def test_kv_query_all(spark_context, riak_client, sql_context):
	_test_spark_rdd_kv_read_query_all(10, spark_context, riak_client, sql_context)

@pytest.mark.riakkv
def test_kv_query_bucket_keys(spark_context, riak_client, sql_context):
	_test_spark_rdd_kv_read_query_bucket_keys(10, spark_context, riak_client, sql_context)

@pytest.mark.riakkv
def test_kv_query_2i_keys(spark_context, riak_client, sql_context):
	_test_spark_rdd_kv_read_query_2i_keys(10, spark_context, riak_client, sql_context)

@pytest.mark.riakkv
def test_kv_query_2i_range(spark_context, riak_client, sql_context):
	_test_spark_rdd_kv_read_query2iRange(10, spark_context, riak_client, sql_context)

@pytest.mark.riakkv
def test_kv_query_partition_by_2i_range(spark_context, riak_client, sql_context):
	_test_spark_rdd_kv_read_partition_by_2i_range(10, spark_context, riak_client, sql_context)

@pytest.mark.riakkv
def test_kv_query_partition_by_2i_keys(spark_context, riak_client, sql_context):
	_test_spark_rdd_kv_read_partition_by_2i_keys(10, spark_context, riak_client, sql_context)

#
# if object values are JSON objects with more than 4 keys exception happens
# https://github.com/basho/spark-riak-connector/issues/206
@pytest.mark.regression
@pytest.mark.riakkv
def test_read_JSON_value_with_more_then_4_fields(spark_context, riak_client):
    bucket = riak_client.bucket("test-bucket-"+str(randint(0,100000)))
    item = bucket.new("test-key")
    item.data = {'field1': 'abc',
                 'field2': 'def',
                 'field3': 'ABC123',
                 'field4': 'home',
                 'field5': '10',
                 'field6': '10.0.0.1',
                 'field7': '1479398907',
                 'field8': '1479398907',
                 'field9': 'DEF456,GHI789',
                 'field11': 'JKL000',
                 'field12': 'abc'}

    item.store()
    result = spark_context.riakBucket(bucket.name).queryBucketKeys("test-key").collect()

#
# if object value is a JSON object that contains a List of values, exception raised
# https://bashoeng.atlassian.net/browse/SPARK-275
#
@pytest.mark.regression
@pytest.mark.riakkv
def test_read_JSON_value_with_an_empty_list (spark_context, riak_client):
	bucket = riak_client.bucket("test-bucket-"+str(randint(0,100000)))
	item = bucket.new("test-key")
	item.data = {u'client_ip': u'35.185.22.50',
				 u'created_time': 1481562884357,
				 u'event_keys': []}

	item.store()
	result = spark_context.riakBucket(bucket.name).queryBucketKeys("test-key").collect()


#
# if object value is a JSON object that contains a List of values, exception raised
# https://bashoeng.atlassian.net/browse/SPARK-275
#
@pytest.mark.regression
@pytest.mark.riakkv
def test_read_JSON_value_with_not_empty_list (spark_context, riak_client):
	bucket = riak_client.bucket("test-bucket-"+str(randint(0,100000)))
	item = bucket.new("test-key")
	item.data = {"session_ids":["t_sess_1401"],
				 "last_active_time":1481562896697,
				 "ecompany":"test.riak.ecompany.com.1"}

	item.store()
	result = spark_context.riakBucket(bucket.name).queryBucketKeys("test-key").collect()

#
# if object value is a JSON object that contains an empty  Object, exception raised
# https://bashoeng.atlassian.net/browse/SPARK-281
#
@pytest.mark.regression
@pytest.mark.riakkv
def test_read_JSON_value_with_an_empty_map (spark_context, riak_client):
	bucket = riak_client.bucket("test-bucket-"+str(randint(0,100000)))
	item = bucket.new("test-key-empty-object")
	item.data = {u'client_ip': u'35.185.22.50',
				 u'created_time': 1481562884357,
				 u'event_keys': {}}

	item.store()
	result = spark_context.riakBucket(bucket.name).queryBucketKeys("test-key-empty-object").collect()
###### TS Tests #######

@pytest.mark.riakts
def test_ts_df_write_use_timestamp(spark_context, riak_client, sql_context):
	_test_spark_df_ts_write_use_timestamp(10, 5, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_write_use_long(spark_context, riak_client, sql_context):
	_test_spark_df_ts_write_use_long(10, 5, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_read_use_timestamp(spark_context, riak_client, sql_context):
	_test_spark_df_ts_read_use_timestamp(10, 5, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_read_use_long(spark_context, riak_client, sql_context):
	_test_spark_df_ts_read_use_long(10, 5, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_read_use_timestamp_ts_quantum(spark_context, riak_client, sql_context):
	_test_spark_df_ts_read_use_timestamp_ts_quantum(10, 5, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_read_use_long_ts_quantum(spark_context, riak_client, sql_context):
	_test_spark_df_ts_read_use_long_ts_quantum(10, 5, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_range_query_input_split_count_use_timestamp(spark_context, riak_client, sql_context):
	_test_spark_df_ts_range_query_input_split_count_use_timestamp(10, 5, 3, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_range_query_input_split_count_use_long(spark_context, riak_client, sql_context):
	_test_spark_df_ts_range_query_input_split_count_use_long(10, 5, 3, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_range_query_input_split_count_use_timestamp_ts_quantum(spark_context, riak_client, sql_context):
	_test_spark_df_ts_range_query_input_split_count_use_timestamp_ts_quantum(10, 5, 3, spark_context, riak_client, sql_context)

@pytest.mark.riakts
def test_ts_df_range_query_input_split_count_use_long_ts_quantum(spark_context, riak_client, sql_context):
	_test_spark_df_ts_range_query_input_split_count_use_long_ts_quantum(10, 5, 3, spark_context, riak_client, sql_context)
