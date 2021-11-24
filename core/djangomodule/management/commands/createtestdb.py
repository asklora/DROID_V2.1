from django.core.management.base import BaseCommand, CommandError
from core.djangomodule.network.cloud import DroidDb
from datasource.rkd import logging

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--delete",
            action="store_true",
            dest="delete",
            help="Delete db instead",
        )
    def handle(self, *args, **options):
        db = DroidDb()
        status =db.check_testdb_status()
        logging.info(status)
        
