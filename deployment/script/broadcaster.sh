#!bin/bash
celery -A core.services worker -l INFO  --concurrency=50 --hostname=broadcaster@%h -Q broadcaster