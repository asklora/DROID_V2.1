#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.settings.development
export EC2HOST=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)
celery -A core.services worker --beat -l  INFO --hostname=dev-master@%h --concurrency=12 --scheduler django_celery_beat.schedulers:DatabaseScheduler