#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.settings.sandbox
export CELERY_ROLE=master
python manage.py migrate --settings=config.settings.sandbox
celery -A core.services worker -l  INFO --hostname=master@%h --concurrency=5