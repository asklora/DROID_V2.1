#!bin/bash
celery -A core.services worker  -l  INFO --hostname=pc1@hk -Q pc1 --concurrency=12