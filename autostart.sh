#!/usr/bin/bash

DATE=`/bin/date +"%Y-%m-%d_%H.%M"`
cd /home/raspberry/Desktop/exchanges_dashboard/
docker-compose up -d
