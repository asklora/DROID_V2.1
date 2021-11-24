#!/bin/bash
# gunicorn config.wsgi:application --bind 0.0.0.0:8000 -w 9 --timeout 360 --log-level=debug
export DJANGO_SETTINGS_MODULE=config.settings.development
# pip install uvicorn[standard]
gunicorn config.asgi:application -k uvicorn.workers.UvicornWorker -w 5 --timeout 360 --log-level=info --bind 0.0.0.0:8000
# daphne config.asgi:application --port 8000 -b 0.0.0.0