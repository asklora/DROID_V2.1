from django.core.management.base import BaseCommand, CommandError
import boto3,yaml,json
import pandas as pd


class Command(BaseCommand):

    def handle(self, *args, **options):
        boto3.setup_default_session(region_name='ap-east-1')
        # file must in the same dir as script
        stack_name = 'cloud-formation'
        cloud_formation_client = boto3.client('cloudformation')
        ec2_client = boto3.client('ec2')
        rds_client = boto3.client('rds')
        print("Creating {}".format(stack_name))
        # response = cloud_formation_client.create_stack(
        #     StackName=stack_name,
        #     TemplateURL='https://cf-templates-ot7ai6np4d71-ap-east-1.s3.ap-east-1.amazonaws.com/2021022ZPO-base-networking.yml'
        # )
        # stack = cloud_formation_client.describe_stacks(StackName=stack_name)
        ec2s = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name','Values': ['bastion',]},])
        rds = rds_client.describe_db_instances(DBInstanceIdentifier='askloraprivatedb')
        # print(response)
        df = pd.DataFrame(columns=['InstanceId', 'InstanceType', 'PrivateIpAddress','PublicIpAddress'])
        i = 0
        for res in ec2s['Reservations']:
            df.loc[i, 'InstanceId'] = res['Instances'][0]['InstanceId']
            df.loc[i, 'InstanceType'] = res['Instances'][0]['InstanceType']
            df.loc[i, 'PrivateIpAddress'] = res['Instances'][0]['PrivateIpAddress']
            df.loc[i, 'PublicIpAddress'] = res['Instances'][0]['PublicIpAddress']
        #     i += 1
        print (rds['DBInstances'][0]['Endpoint']['Address'])
        print (rds['DBInstances'][0]['Endpoint']['Port'])
        print (rds['DBInstances'][0]['MasterUsername'])
        print(df)
