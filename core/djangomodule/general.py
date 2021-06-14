import base64
import os
from django.utils.deconstruct import deconstructible
import boto3
import time
from config.celery import app

def is_hashed(pwd):
    if 'pbkdf2_sha256$216000$' in pwd:
        return True
    return False
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


def aws_batch(func):

    def inner():
        print('===== CREATING BATCH =====')
        batch_job_definition = 'test-celery-job'
        batch_queue = 'getting-started-job-queue'
        host_name = 'portfolio-job@aws-batch'
        client = boto3.client('batch', region_name='ap-northeast-2')
        submit_job = client.submit_job(
            jobName='jobfromboto',
            jobQueue=batch_queue,
            jobDefinition=batch_job_definition,
            containerOverrides={
                'vcpus': 128,
                'memory': 32000,
                'command': [
                    "celery", "-A", "core.services", "worker", "-l", "INFO", f"--hostname={host_name}", "-Q", "batch"
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
            if response['jobs'][0]['status'] in ['RUNNING']:
                active_node = app.control.ping(timeout=0.5)
                for node in active_node:
                    if 'portfolio-job@aws-batch' in node:
                        if node['portfolio-job@aws-batch']['ok'] == 'pong':
                            print('waiting for queue')
                            status = 'READY'
            if status == 'READY':
                break
            if status != response['jobs'][0]['status']:
                print('state is ' + response['jobs'][0]['status'])
                status = response['jobs'][0]['status']
            time.sleep(10)
        if status == 'READY':
            func.apply_async(queue='batch')
            time.sleep(2)

        while True:
            task = app.control.inspect([host_name]).active()
            active_task = len(task[host_name])
            print(f'task active: {active_task}')
            if active_task == 0:
                terminate_command = client.terminate_job(
                    jobId=submit_job["jobId"],
                    reason='task done'
                )
                print(terminate_command)
                print('==== DONE MIGRATING =====')
                break
            time.sleep(15)
    return inner


def nonetozero(value):
    if value:
        return value
    return 0


def formatdigit(value, currency_decimal=True):
    # digit = max(min(5 - len(str(int(value))), 2), -1)
    if(currency_decimal):
        return round(value, 2)
    else:
        return round(value, 0)
