#!bin/bash
celery -A core.services worker -l INFO --hostname=EC2@%h -Q EC2