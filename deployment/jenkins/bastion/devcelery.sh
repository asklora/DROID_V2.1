#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.development
python manage.py celery_set_default_queue -q droid_dev
celery -A core.services worker --beat -l  INFO --hostname=dev-master@%h --concurrency=12 --scheduler django_celery_beat.schedulers:DatabaseScheduler