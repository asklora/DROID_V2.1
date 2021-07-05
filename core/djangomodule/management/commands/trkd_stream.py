from datetime import datetime
import boto3
import time
import sys
import getopt
import socket
import json
import requests
import websocket
from datasource.rkd import Rkd
from core.universe.models import Universe
from django.core.management.base import BaseCommand, CommandError
from datasource.rkd import RkdStream
"""
Global Variables
position : remote address / your IP

"""
# Start websocket handshake


class Command(BaseCommand):

    HKD_universe = [ticker['ticker'] for ticker in Universe.objects.filter(
        currency_code__in=['EUR'], is_active=True).values('ticker')]
    rkd = RkdStream(HKD_universe)
    quotes = rkd.stream()
