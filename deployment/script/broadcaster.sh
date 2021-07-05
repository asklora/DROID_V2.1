#!bin/bash
celery -A core.services worker -l INFO --hostname=dev@%h -Q broadcaster