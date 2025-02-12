import base64
import os
from django.utils.deconstruct import deconstructible
from rest_framework import serializers
import boto3
import time
from django.core.cache import cache
import json
from rest_framework import permissions,status
from rest_framework.exceptions import APIException
from django_redis import get_redis_connection
from redis.exceptions import ConnectionError
import logging,coloredlogs
import pandas as pd
import pprint
from rest_framework.throttling import UserRateThrottle
from django.utils import timezone
import calendar
from datetime import timedelta

coloredlogs.install()
logging.basicConfig(format="%(asctime)s - %(message)s",
                    datefmt="%d-%b-%y %H:%M:%S")
logging.getLogger().setLevel(logging.INFO)



def is_end_of_month(date:timezone=timezone.now().date()) -> bool:
    end_month = date.replace(day = calendar.monthrange(date.year, date.month)[1])
    tommorow_date = end_month+ timedelta(days=1)
    return int(tommorow_date.strftime("%d")) == 1


class NeedRegister(APIException):
    """
    change 401 to 403
    """
    status_code = status.HTTP_403_FORBIDDEN
    default_detail = {'detail': 'User is not Registered or has permission'}
    default_code = 'credentials_error'


class OrderActionThrottle(UserRateThrottle):
    scope = 'order_action'

    def parse_rate(self, rate):
        """
        Given the request rate string, return a two tuple of:
        <allowed number of requests>, <period of time in seconds>

        So we always return a rate for 1 request per 6 second. to prevent burst order

        Args:
            string: rate to be parsed, which we ignore.

        Returns:
            tuple:  <allowed number of requests>, <period of time in seconds>
        """
        return (1, 5)

    def allow_request(self, request, view):
        """
        Implement the check to see if the request should be throttled.

        On success calls `throttle_success`.
        On failure calls `throttle_failure`.
        """
        if self.rate is None:
            return True

        self.key = self.get_cache_key(request, view)
        if self.key is None:
            return True

        self.history = self.cache.get(self.key, [])
        self.now = self.timer()

        # Drop any requests from the history which have now passed the
        # throttle duration

        while self.history and self.history[-1] <= self.now - self.duration:
            self.history.pop()
        if len(self.history) >= self.num_requests:
            return self.throttle_failure()
        return self.throttle_success()


class OrderThrottle(UserRateThrottle):
    scope = 'order'

    def parse_rate(self, rate):
        """
        Given the request rate string, return a two tuple of:
        <allowed number of requests>, <period of time in seconds>

        So we always return a rate for 1 request per 6 second. to prevent burst order

        Args:
            string: rate to be parsed, which we ignore.

        Returns:
            tuple:  <allowed number of requests>, <period of time in seconds>
        """
        return (1, 5)

    def allow_request(self, request, view):
        """
        Implement the check to see if the request should be throttled.

        On success calls `throttle_success`.
        On failure calls `throttle_failure`.
        """
        if self.rate is None:
            return True

        self.key = self.get_cache_key(request, view)
        if self.key is None:
            return True

        self.history = self.cache.get(self.key, [])
        self.now = self.timer()

        # Drop any requests from the history which have now passed the
        # throttle duration
        print(self.num_requests)

        while self.history and self.history[-1] <= self.now - self.duration:
            self.history.pop()
        if len(self.history) >= self.num_requests:
            return self.throttle_failure()
        return self.throttle_success()

    
class IsRegisteredUser(permissions.BasePermission):
    message = 'User is not Registered or has permission'

    def has_permission(self, request, view):
        if request.user.is_anonymous:
            return NeedRegister()
        return True

def jsonprint(data:dict)->None:
    pretty =pprint.PrettyPrinter(indent=2)
    pretty.pprint(data)
@deconstructible
class UploadTo:
    def __init__(self, name):
        self.name = name

    def __call__(self, instance, filename):
        ext = filename.split(".")[-1]
        filename = f"{instance.app.creator.first_name.upper()}_{instance.app.creator.last_name.upper()}_{self.name}_{instance.app.uid}.{ext}"
        return "{0}/{1}".format(instance.app.app_type.name, filename)


def generate_id(digit):
    r_id = base64.b64encode(os.urandom(digit)).decode("ascii")
    r_id = r_id.replace(
        "/", "").replace("_", "").replace("+", "").replace("=", "").strip()
    return r_id



def nonetozero(value):
    if value:
        return value
    return 0

def is_hashed(pwd):
    if 'pbkdf2_sha256$216000$' in pwd:
        return True
    return False


def formatdigit(value, currency_decimal=True):
    # digit = max(min(5 - len(str(int(value))), 2), -1)
    if(currency_decimal):
        return round(value, 2)
    else:
        return round(value, 0)


def run_batch():
    print('===== CREATING BATCH =====')
    batch_job_definition = 'dlp-db'
    batch_queue = 'dlp-queue'
    client = boto3.client('batch', region_name='ap-northeast-2')
    submit_job = client.submit_job(
        jobName='run_master',
        jobQueue=batch_queue,
        jobDefinition=batch_job_definition,
        containerOverrides={
            'vcpus': 4,
            'memory': 64000,
            'command': [
                "python", "master.py"
            ]
        },
        timeout={
            'attemptDurationSeconds': 5000
        })
    print('===== JOB SUBMITTED =====')
    print(f'JOB ID : {submit_job["jobId"]}')
    status = ''
    while True:
        response = client.describe_jobs(
            jobs=[
                submit_job['jobId'],
            ]
        )
        if response['jobs'][0]['status'] in ['FAILED', 'SUCCEEDED']:
            print(response['jobs'][0]['status'])
            break
        if status != response['jobs'][0]['status']:
            print('state is ' + response['jobs'][0]['status'])
            status = response['jobs'][0]['status']
        time.sleep(10)


def symbol_hkd_fix(symbol:str) ->str:
    fix_length = 4
    if len(symbol) != fix_length:
        add = fix_length - len(symbol)
        additional_zero = "0" * add
        return f'{additional_zero}{symbol}'
    return symbol


def flush_cache():
    try:
        con =get_redis_connection("default")
        con.flushall()
        logging.info("cache flushed")
    except ConnectionError:
        logging.error("Redis isn't running. skip get cache`")


def get_cached_data(key,df=False):
    
    try:
        get_redis_connection("default")
        cached_data = cache.get(key)
        if cached_data:
            data= json.loads(cached_data)
            if df:
                if isinstance(data,list):
                    data = pd.DataFrame(data)
                elif isinstance(data,dict):
                    data = pd.DataFrame([data])
                else:
                    raise ValueError('dataframe should be a dict or list')
            return data
    except ConnectionError:
        logging.error("Redis isn't running. skip get cache`")
    except Exception as e:
        logging.info(e)
    return False


def set_cache_data(key,data=None,interval=60*60):
    try:
        get_redis_connection("default")
        cache.set(key,json.dumps(data),interval)
    except ConnectionError:
        logging.error("Redis isn't running. ignoring set cache ")
    except Exception as e:
        logging.info(e)
    



class UnixEpochDateField(serializers.DateTimeField):
    def to_native(self, value):
        """ Return epoch time for a datetime object or ``None``"""
        import time
        try:
            return int(time.mktime(value.timetuple()))
        except (AttributeError, TypeError):
            return None

    def from_native(self, value):
        import datetime
        return datetime.datetime.fromtimestamp(int(value))

class errserializer(serializers.Serializer):
    detail = serializers.CharField()
