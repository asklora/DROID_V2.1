#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.settings.sandbox
export CELERY_ROLE=master
celery -A core.services worker -l  INFO --hostname=master@%h --concurrency=5