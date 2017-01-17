import os
import pytest
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession
import riak, pyspark_riak

@pytest.fixture(scope="session")
def docker_cli(request):
    # Start spark context to get access to py4j gateway
    conf = SparkConf().setMaster("local[*]").setAppName("pytest-pyspark-py4j")
    sparkSession = SparkSession.builder.config(conf).getOrCreate()
    sc = sparkSession.sparkContext
    docker_cli = sc._gateway.jvm.com.basho.riak.test.cluster.DockerRiakCluster(1, 2)
    docker_cli.start()
    sc.stop()
    # Start spark context since it's not aware of riak nodes and thus can't be used to test riak
    request.addfinalizer(lambda: docker_cli.stop())
    return docker_cli

@pytest.fixture(scope="session")
def spark_session(request):
    if not os.environ.has_key('RIAK_HOSTS'):
        docker_cli = request.getfuncargvalue('docker_cli')
        host_and_port = get_host_and_port(docker_cli)
        os.environ['RIAK_HOSTS'] = host_and_port
        os.environ['USE_DOCKER'] = 'true'
    # Start new spark context
    conf = SparkConf().setMaster('local[*]').setAppName('pytest-pyspark-local-testing')
    conf.set('spark.riak.connection.host', os.environ['RIAK_HOSTS'])
    conf.set('spark.driver.memory', '4g')
    conf.set('spark.executor.memory', '4g')
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark_session


@pytest.fixture(scope="session")
def spark_context(request):
    # If RIAK_HOSTS is not set, use Docker to start a Riak node
    if not os.environ.has_key('RIAK_HOSTS'):
      docker_cli = request.getfuncargvalue('docker_cli')
      host_and_port = get_host_and_port(docker_cli)
      os.environ['RIAK_HOSTS'] = host_and_port
      os.environ['USE_DOCKER'] = 'true'
    # Start new spark context
    conf = SparkConf().setMaster('local[*]').setAppName('pytest-pyspark-local-testing')
    conf.set('spark.riak.connection.host', os.environ['RIAK_HOSTS'])
    conf.set('spark.driver.memory', '4g')
    conf.set('spark.executor.memory', '4g')
    spark_context = SparkSession.builder.config(conf=conf).getOrCreate().sparkContext
    spark_context.setLogLevel('INFO')
    pyspark_riak.riak_context(spark_context)
    request.addfinalizer(lambda: spark_context.stop())
    return spark_context


@pytest.fixture(scope="session")
def riak_client(request):
    nodes = []
    if os.environ.has_key('USE_DOCKER'):
      docker_cli = request.getfuncargvalue("docker_cli")
      nodes = get_nodes(docker_cli)
    else:
      nodes =[{'protocol': "pbc", 'host': t[0], 'pb_port': t[1] if len(t)>1 else "8087"} for t in  [tuple(x.strip().split(':')) for x in os.environ['RIAK_HOSTS'].split(',') ]]
    client = riak.RiakClient(nodes=nodes)
    request.addfinalizer(lambda: client.close())

    client.ping()
    return client

def get_nodes(docker_cli):
    pb_port = 8087
    http_port = 8098
    nodes = [{'host': ip, 'pb_port': pb_port, 'http_port': http_port} for ip in docker_cli.getIps()]
    return nodes

def get_host_and_port(docker_cli):
    pb_port = 8087
    nodes = [":".join([ip, str(pb_port)]) for ip in docker_cli.getIps()]
    host_and_port= ",".join(nodes)
    return host_and_port
