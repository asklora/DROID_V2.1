import pandas as pd
from django.core.management.base import BaseCommand
from core.services.models import ThirdpartyCredentials
from django.db import transaction


class Command(BaseCommand):

    def handle(self, *args, **options):
        with transaction.atomic():
            # try:
        # transaction.set_autocommit(False)
            ThirdpartyCredentials.objects.create(services='asjhd')
            transaction.set_rollback(True)
        transaction.rollback()
                # raise ValueError('sss')
            # except Exception:
        # transaction.set_rollback(True)
