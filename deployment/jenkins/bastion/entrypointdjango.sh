#!bin/bash
# gunicorn config.wsgi:application --bind 0.0.0.0:8000 -w 9 --timeout 360 --log-level=debug
pip install uvicorn[standard]
# gunicorn config.asgi:application -k uvicorn.workers.UvicornWorker -w 9 --timeout 360 --log-level=debug --bind 0.0.0.0:8000
daphne config.asgi:application