#!/bin/bash
cd /home/ubuntu/DROID_V2.1
docker-compose -p droid -f deployment/compose/droid.yml up --build --force-recreate -d
sudo chown -R ubuntu:ubuntu /home/ubuntu/DROID_V2.1