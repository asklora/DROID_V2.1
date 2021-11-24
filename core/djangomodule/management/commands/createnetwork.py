from django.core.management.base import BaseCommand, CommandError
from ...network.cloud import Cloud,DroidDb
import boto3,yaml,json

class Command(BaseCommand):

    def handle(self, *args, **options):
        cloud = DroidDb()
        cloud.create_read_replica()