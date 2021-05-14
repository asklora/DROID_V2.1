from migrate import currency
from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
from core.services.ingestiontask import migrate_droid1
from core.services.tasks import send_csv_hanwha,populate_client_top_stock_weekly,order_client_topstock,daily_hedge,populate_latest_price
from main import populate_intraday_latest_price

class Command(BaseCommand):

    def handle(self, *args, **options):
        populate_latest_price(currency_code="USD")
#         send_csv_hanwha(currency=currency,new={'pos_list':[
#             '5553f811-f79f-4dcd-bbf2-fc7d3662213d',
#             '3e185bde-ea23-4230-bd49-59200fbb12fa',
#             '5af3df3a-e14d-496b-b21b-5c0c0701649c',
#             '49bbed1c-cc3a-4444-96cc-f01d88b8c3f0',
#             '5142bc48-dd46-41e6-8381-7b25860988c9',
#             'd3521289-3d1f-48f0-85d3-67410afcc545'
# ]})
        daily_hedge(currency="USD")
        # print(user.client_user.all()[0].client.client_uid)
        # migrate_droid1.apply_async(queue='droid')
