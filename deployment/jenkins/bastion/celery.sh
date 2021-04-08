#!bin/bash
celery -A core.services worker -B -l  INFO --hostname=red-pc@%h -Q EC2