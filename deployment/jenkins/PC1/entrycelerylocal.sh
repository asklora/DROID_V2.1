#!bin/bash
celery -A core.services worker -l INFO --hostname=PC1@%h -Q PC1