#!/bin/bash
export DJANGO_SETTINGS_MODULE=config.production
celery -A core.services worker -l INFO  --concurrency=20 --hostname=broadcaster@%h -Q broadcaster