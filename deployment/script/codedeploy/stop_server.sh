#!/bin/bash
cd /home/ubuntu/DROID_V2.1
docker exec -it django bash -c "python manage.py send_slack_message -m 'provisioning blue/green, container stop :x:'"
docker-compose -p droid -f deployment/compose/droid.yml down
docker system prune -a -f && docker image prune -a -f 
sudo rm -rf /home/ubuntu/DROID_V2.1