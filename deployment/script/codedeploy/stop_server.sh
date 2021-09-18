#!/bin/bash
docker-compose -p droid -f deployment/compose/droid.yml down
docker system prune -a -f && docker image prune -a -f 
sudo rm -rf /home/ubuntu/DROID_V2.1
mkdir DROID_V2.1
# cd /home/ubuntu/ && git clone git@github.com:asklora/DROID_V2.1.git