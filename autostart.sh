#!/usr/bin/bash

DATE=`/bin/date +"%Y-%m-%d_%H.%M"`
cd /repos/exchanges_dashboard/
docker-compose up -d
