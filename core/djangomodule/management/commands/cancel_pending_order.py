from django.core.management.base import BaseCommand
from datetime import timedelta,datetime
from core.services.order_services import cancel_pending_order


class Command(BaseCommand):
    def handle(self, *args, **options):
        date = datetime.now() - timedelta(days=1)
        cancel_pending_order(from_date=date)


