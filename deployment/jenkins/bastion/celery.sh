#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.settings.production
celery -A core.services worker --beat -l  INFO --hostname=master@%h --concurrency=10 --scheduler django_celery_beat.schedulers:DatabaseScheduler