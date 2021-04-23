#!/bin/bash

echo Building docker images...

pushd `dirname $0`

docker build -t spark-base \
--build-arg aws_access_key_id=$AWS_ACCESS_KEY_ID \
--build-arg aws_secret_access_key=$AWS_SECRET_ACCESS_KEY \
 docker/base

docker build -t spark-node docker/node
docker build -t spark-worker docker/worker
docker build -t spark-master docker/master
docker build -t spark-client docker/client

popd

echo Docker images are built
