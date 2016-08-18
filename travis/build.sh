#!/bin/bash

SBT_CMD="sbt -Dcom.basho.riak.pbchost=$RIAK_HOSTS ++$TRAVIS_SCALA_VERSION"

set -e

if [ "$RIAK_FLAVOR" == "riak-kv" ]; then
  $SBT_CMD runRiakKVTests
else
  python -m ensurepip
  pip install --upgrade pip setuptools pytest findspark riak timeout_decorator tzlocal
  $SBT_CMD assembly spPackage test runPySparkTests
fi

if [ "develop" == "$TRAVIS_BRANCH" -o "master" == "$TRAVIS_BRANCH" ] && [ "false" == "$TRAVIS_PULL_REQUEST" ]; then
  openssl aes-256-cbc -K $encrypted_e4898ca93742_key -iv $encrypted_e4898ca93742_iv -in secrets.tgz.enc -out secrets.tgz -d && tar -zxf secrets.tgz
  $SBT_CMD sparkRiakConnector/publishSigned
fi
