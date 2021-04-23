#!/bin/bash

echo Starting spark cluster...

pushd `dirname $0`
docker-compose up -d master worker-1 worker-2

# starts hdfs cluster
docker exec `docker-compose ps -q master` /home/hadoop/hadoop-3.2.2/sbin/start-dfs.sh
popd

echo Spark cluster started.
