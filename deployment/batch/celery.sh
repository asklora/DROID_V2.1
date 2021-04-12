#!bin/bash
celery -A core.services worker -l INFO --hostname=portfolio-job@aws-batch -Q batch