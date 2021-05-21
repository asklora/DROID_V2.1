from migrate import currency
from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
from core.djangomodule.yahooFin import get_quote_index,scrap_csi
from core.orders.models import PositionPerformance, Order
from core.services.ingestiontask import migrate_droid1
from core.services.tasks import send_csv_hanwha, populate_client_top_stock_weekly, order_client_topstock, daily_hedge, populate_latest_price,get_quote_yahoo
from main import populate_intraday_latest_price,update_index_price_from_dss
from datetime import datetime



class Command(BaseCommand):

    def handle(self, *args, **options):
        get_quote_yahoo("TCOM", use_symbol=True)
        # populate_client_top_stock_weekly(currency="KRW")
        # orders = [ids.order_uid for ids in Order.objects.filter(is_init=True)]
        # perf = PositionPerformance.objects.filter(
        #     position_uid__user_id__in=[108,
        #                                109,
        #                                110], created__gte=datetime.now().date(), position_uid__ticker__currency_code="KRW").order_by("created")
        # perf = perf.exclude(order_uid__in=orders)
        # print(perf.count())
        # populate_latest_price(currency_code="KRW")
        #         send_csv_hanwha(currency=currency,new={'pos_list':[
        #             '5553f811-f79f-4dcd-bbf2-fc7d3662213d',
        #             '3e185bde-ea23-4230-bd49-59200fbb12fa',
        #             '5af3df3a-e14d-496b-b21b-5c0c0701649c',
        #             '49bbed1c-cc3a-4444-96cc-f01d88b8c3f0',
        #             '5142bc48-dd46-41e6-8381-7b25860988c9',
        #             'd3521289-3d1f-48f0-85d3-67410afcc545'
        # ]})
        # populate_intraday_latest_price(currency_code=["KRW"])
        # print(user.client_user.all()[0].client.client_uid)
        # migrate_droid1.apply_async(queue='droid')
        # print(daily_hedge(currency="KRW"))
