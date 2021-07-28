import pandas as pd
from django.core.management.base import BaseCommand
from core.services.models import ThirdpartyCredentials
from django.db import transaction


class Command(BaseCommand):

    def handle(self, *args, **options):
        with transaction.atomic():
            sid =transaction.savepoint()
            # try:
            ThirdpartyCredentials.objects.create(services='asdad')
                # raise ValueError('sss')
            # except Exception:
            transaction.savepoint_rollback(sid)

