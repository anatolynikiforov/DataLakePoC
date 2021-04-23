#!/bin/bash

echo Stopping spark cluster...

pushd `dirname $0`
docker-compose down -v
popd
