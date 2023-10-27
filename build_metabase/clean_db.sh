#!/bin/bash

docker build -f ./build_metabase/Dockerfile_cleaner -t metabase_cleaner .
docker run -v "$(pwd):/code" metabase_cleaner
