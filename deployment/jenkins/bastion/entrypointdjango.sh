#!/bin/bash
# gunicorn config.wsgi:application --bind 0.0.0.0:8000 -w 9 --timeout 360 --log-level=debug
# pip install uvicorn[standard]
# gunicorn config.asgi:application -k uvicorn.workers.UvicornWorker -w 9 --timeout 360 --log-level=debug --bind 0.0.0.0:8000
export EC2HOST=$(curl http://169.254.169.254/latest/meta-data/public-ipv4)

daphne config.asgi:application --port 8000 -b 0.0.0.0