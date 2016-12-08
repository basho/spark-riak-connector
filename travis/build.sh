#!/bin/bash

set -e

if [ -z ${RIAK_HOSTS+x} ]; then
    echo "RIAK_HOSTS is not set, Docker will be used to spin up Riak nodes"
    export RIAK_HOSTS=$(docker inspect -f '{{.NetworkSettings.IPAddress}}' riak-ts):8087,$(docker inspect -f '{{.NetworkSettings.IPAddress}}' riak-ts-2):8087,$(docker inspect -f '{{.NetworkSettings.IPAddress}}' riak-ts-3):8087
fi

#,$(docker inspect -f '{{.NetworkSettings.IPAddress}}' riak-ts-4):8087,$(docker inspect -f '{{.NetworkSettings.IPAddress}}' riak-ts-5):8087

SBT_CMD="sbt -Dcom.basho.riak.pbchost=$RIAK_HOSTS ++$TRAVIS_SCALA_VERSION"

$SBT_CMD package assembly spPackage

python -m ensurepip
pip install --upgrade pip setuptools pytest findspark riak timeout_decorator tzlocal

if [ "$RIAK_FLAVOR" == "riak-kv" ]; then
  $SBT_CMD runRiakKVTests "runPySparkTests kv-tests-only"
else
  $SBT_CMD test runPySparkTests

	if [[ $TRAVIS_BRANCH =~ release-.* || "develop" == "$TRAVIS_BRANCH" ]] && [ "false" == "$TRAVIS_PULL_REQUEST" ]; then
		openssl aes-256-cbc -K $encrypted_d60a16d52fe8_key -iv $encrypted_d60a16d52fe8_iv -in secrets.tgz.enc -out secrets.tgz -d && tar -zxf secrets.tgz
		$SBT_CMD sparkRiakConnector/publishSigned
	fi
fi
