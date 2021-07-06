#!bin/bash
celery -A core.services worker -l INFO  --concurrency=20 --hostname=broadcaster@%h -Q broadcaster