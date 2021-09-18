#!/bin/bash
cd /home/ubuntu/DROID_V2.1
pwd
docker-compose -p droid -f deployment/compose/droid.yml up --build --force-recreate -d