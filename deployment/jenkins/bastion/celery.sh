#!bin/bash
celery -A core.services worker --beat -l  INFO --hostname=master@%h --concurrency=12 --scheduler django_celery_beat.schedulers:DatabaseScheduler