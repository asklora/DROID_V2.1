#!bin/bash
celery -A core.services worker -l INFO --hostname=%h@aws-batch -Q batch