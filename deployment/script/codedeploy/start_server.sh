#!/bin/bash
cd /home/ubuntu/DROID_V2.1
docker-compose -p droid -f deployment/compose/droid.yml up --build --force-recreate -d
docker exec -i django bash -c "python manage.py send_slack_message -m 'provisioning blue/green, container up :white_check_mark:'"
sudo chown -R ubuntu:ubuntu /home/ubuntu/DROID_V2.1
cd /home/ubuntu/DROID_V2.1/deployment/ansible
docker exec -i django bash -c "python manage.py send_slack_message -m 'update alibaba container :warning:'"
ansible-playbook alibabaworker.yml -i inventory.yml
docker exec -i django bash -c "python manage.py send_slack_message -m 'alibaba container updated :white_check_mark:'"