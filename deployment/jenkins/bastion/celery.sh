#!bin/bash
celery -A core.services worker -l -B INFO --hostname=EC2@%h -Q EC2