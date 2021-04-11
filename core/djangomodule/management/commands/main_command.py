from django.core.management.base import BaseCommand, CommandError
from config.celery import app
import boto3
import time
import sys


def executetask(client,jobid,hostname):
    payload = [
        {
        'type':'invoke',
        'module':'migrate.vix_data'
        },{
        'type':'invoke',
        'module':'migrate.data_dss'
        },{
        'type':'invoke',
        'module':'migrate.data_dsws'
        },{
        'type':'invoke',
        'module':'migrate.data_quandl'
        },{
        'type':'invoke',
        'module':'migrate.latest_price'
        },{
        'type':'invoke',
        'module':'migrate.data_interest'
        },
               ]
    for data in payload:
        app.send_task('config.celery.listener',args=(data,),queue='batch')
    while True:
        task = app.control.inspect([hostname]).active()
        active_task = len(task[hostname])
        print(f'task active: {active_task}')
        if active_task == 0:
            terminate_command = client.terminate_job(
                jobId=jobid,
                reason='task done'
            )
            print(terminate_command)
            print('==== DONE MIGRATING =====')
            break
        time.sleep(15)


class Command(BaseCommand):

    def handle(self, *args, **options):
        # node = app.control.inspect()
        # print(app.control.ping(timeout=0.5))
        # print(node.scheduled())
        print('===== CREATING BATCH =====')
        # batch_env = 'getting-started-compute-envs'
        batch_job_definition ='test-celery-job'
        batch_queue = 'getting-started-job-queue'
        host_name='portfolio-job@aws-batch'
        client = boto3.client('batch',region_name='ap-northeast-2')
        submit_job =client.submit_job(
                        jobName='jobfromboto',
                        jobQueue=batch_queue,
                        jobDefinition=batch_job_definition,
                        containerOverrides={
                            'vcpus': 256,
                            'memory': 3200,
                            'command': [
                                "celery","-A","core.services","worker","-l","INFO",f"--hostname={host_name}","-Q","batch"
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
            if response['jobs'][0]['status'] in ['FAILED','SUCCEEDED']:
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
        print(status)
        if status == 'READY':
            executetask(client,submit_job["jobId"],host_name)