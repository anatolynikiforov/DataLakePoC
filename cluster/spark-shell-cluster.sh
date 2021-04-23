#!/bin/bash

pushd `dirname $0`
docker-compose run client
popd
