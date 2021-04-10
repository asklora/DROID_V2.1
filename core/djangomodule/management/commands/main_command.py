from django.core.management.base import BaseCommand, CommandError
from config.celery import app
import boto3
import time
import sys
class Command(BaseCommand):

    def handle(self, *args, **options):
        # node = app.control.inspect()
        # print(app.control.ping(timeout=0.5))
        # print(node.scheduled())
        print('===== CREATING BATCH =====')
        # batch_env = 'getting-started-compute-envs'
        batch_job_definition ='test-celery-job'
        batch_queue = 'getting-started-job-queue'
        client = boto3.client('batch',region_name='ap-northeast-2')
        submit_job =client.submit_job(
                        jobName='jobfromboto',
                        jobQueue=batch_queue,
                        jobDefinition=batch_job_definition,
                        containerOverrides={
                            'vcpus': 200,
                            'memory': 8000,
                            'command': [
                                "celery","-A","core.services","worker","-l","INFO","--hostname=portfolio-job@aws-batch","-Q","batch"
                            ]
                        },
                        timeout={
                            'attemptDurationSeconds': 60
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
            if response['jobs'][0]['status'] in ['FAILED','SUCCEEDED']:
                print(response['jobs'][0]['status'])
                break
            if response['jobs'][0]['status'] in ['RUNNING']:
                active_node = app.control.ping(timeout=0.5)
                for node in active_node:
                    if 'portfolio-job@aws-batch' in node:
                        if node['portfolio-job@aws-batch']['ok'] == 'pong':
                            print('waiting for queue')
            if status != response['jobs'][0]['status']:
                print('state is ' + response['jobs'][0]['status'])
                status = response['jobs'][0]['status']
            time.sleep(10)