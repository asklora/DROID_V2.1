from migrate import currency
from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
from core.services.ingestiontask import migrate_droid1
from core.services.tasks import send_csv_hanwha,populate_client_top_stock_weekly,order_client_topstock


class Command(BaseCommand):

    def handle(self, *args, **options):
        populate_client_top_stock_weekly(currency="HKD")
        order_client_topstock(currency="HKD")
        # print(user.client_user.all()[0].client.client_uid)
        # migrate_droid1.apply_async(queue='droid')
