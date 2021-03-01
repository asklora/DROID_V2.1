from django.core.management.base import BaseCommand, CommandError
from ...network.cloud import Cloud,DroidDb
import boto3,yaml,json

class Command(BaseCommand):

    def handle(self, *args, **options):
        boto3.setup_default_session(region_name='ap-east-1')
        rds_client = boto3.client('rds')
        db = DroidDb()
        db.create_test_db(create_new=True)