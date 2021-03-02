from django.core.management.base import BaseCommand, CommandError
from ...network.cloud import Cloud
import boto3,yaml,json

class Command(BaseCommand):

    def handle(self, *args, **options):
        cloud = Cloud()
        cloud.create_stack()