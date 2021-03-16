from django.core.management.base import BaseCommand, CommandError
from core.djangomodule.network.cloud import DroidDb


class Command(BaseCommand):
    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument(
            '--delete',
            action='store_true',
            dest='delete',
            help='Delete db instead',
        )
    def handle(self, *args, **options):
        db = DroidDb()
        if options['delete']:
            db.delete_old_testdb()
        else:
            db.create_test_db()
