#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.settings.production
celery -A core.services worker -l INFO  --concurrency=5 --hostname=broadcaster@%h -Q broadcaster