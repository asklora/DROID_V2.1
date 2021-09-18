#!/bin/bash
docker-compose -p droid -f deployment/compose/droid.yml up --build --force-recreate -d