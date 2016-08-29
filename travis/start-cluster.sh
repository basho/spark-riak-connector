#!/bin/bash

set -e

COORDINATOR_CONTAINER=$(docker run --name riak-ts -d -p 8087:8087 -p 8098:8098 basho/riak-ts)
COORDINATOR_HOSTNAME=$(docker inspect -f '{{.NetworkSettings.IPAddress}}' $COORDINATOR_CONTAINER)

docker exec $COORDINATOR_CONTAINER riak-admin wait-for-service riak_kv

CLUSTER_NODES=$COORDINATOR_CONTAINER
CLUSTER_NODES+=" $(docker run --name riak-ts-2 -d -e COORDINATOR_NODE=$COORDINATOR_HOSTNAME -P basho/riak-ts)"
CLUSTER_NODES+=" $(docker run --name riak-ts-3 -d -e COORDINATOR_NODE=$COORDINATOR_HOSTNAME -P basho/riak-ts)"
#CLUSTER_NODES+=" $(docker run --name riak-ts-4 -d -e COORDINATOR_NODE=$COORDINATOR_HOSTNAME -P basho/riak-ts)"
#CLUSTER_NODES+=" $(docker run --name riak-ts-5 -d -e COORDINATOR_NODE=$COORDINATOR_HOSTNAME -P basho/riak-ts)"

echo $CLUSTER_NODES

for node in $CLUSTER_NODES; do
	docker exec $node riak-admin wait-for-service riak_kv
done
