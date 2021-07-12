#!/bin/bash

echo Building airflow docker image...

pushd `dirname $0`

docker build -t spark-airflow \
--build-arg aws_access_key_id=$AWS_ACCESS_KEY_ID \
--build-arg aws_secret_access_key=$AWS_SECRET_ACCESS_KEY \
 .

popd

echo Airflow docker image has been built
