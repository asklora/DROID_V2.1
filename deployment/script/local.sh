#!/bin/bash
# gunicorn config.wsgi:application --bind 0.0.0.0:8000 -w 9 --timeout 360 --log-level=debug
export DJANGO_SETTINGS_MODULE=config.settings.sandbox
python manage.py runserver 0.0.0.0:8000