#!bin/bash
celery -A core.services worker -B -l  INFO --hostname=master@bastion -Q EC2