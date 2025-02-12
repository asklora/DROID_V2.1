#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.settings.development
export CELERY_ROLE=master

celery -A core.services worker --beat -l  INFO --hostname=dev-master@%h --concurrency=12 --scheduler django_celery_beat.schedulers:DatabaseScheduler