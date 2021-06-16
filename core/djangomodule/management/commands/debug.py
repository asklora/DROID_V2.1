from migrate import currency
from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
from core.djangomodule.yahooFin import get_quote_index,scrap_csi
from core.djangomodule.calendar import TradingHours
from core.orders.models import PositionPerformance, Order,OrderPosition
from core.Clients.models import ClientTopStock
from core.services.ingestiontask import migrate_droid1
from core.services.tasks import send_csv_hanwha, populate_client_top_stock_weekly, order_client_topstock, daily_hedge, populate_latest_price,get_quote_yahoo,update_index_price_from_dss
from main import populate_intraday_latest_price,update_index_price_from_dss
from datetime import datetime
from datasource.rkd import RkdData


class Command(BaseCommand):
    def handle(self, *args, **options):
        # user = User.objects.get(id=1)
        # print(user.check_password('pbkdf2_sha256$216000$SOyf9SnnXmzC$tpeNQM5F/AFhMMJNFnkZz='))
        rkd = RkdData()
        quotes = rkd.get_quote(['003000.KS','000066.SZ'],save=True)
        # quotes.save('master','LatestPrice')
        # order_client_topstock(currency="KRW", client_name="HANWHA")
        # odrs=OrderPosition.objects.filter()
        # for odr in odrs:
        #     odr.save()
        # user = User.objects.get(id=108)
        # print(user.total_amount)
        # print(user.current_total_investment_value)
        # print(user.total_invested_amount)
        # print(user.total_profit_return)
        # print(user.total_fee_amount)

        # for item in ClientTopStock.objects.all():
        #     day = item.spot_date
        #     year = day.isocalendar()[0]
        #     week = day.isocalendar()[1]
        #     interval = f'{year}{week}'
        #     item.week_of_year = int(interval)
        #     item.save()
        #     print(interval)
        # print(year,week, day.weekday())
        # migrate_droid1("na")
        # print(func(25,45,11))
        # user = User.objects.get(email='krw_s_adv@hanwha.asklora.ai')
        # print('current_assets: ',user.current_assets)
        # print('balance: ',user.balance)
        # print('position_live: ',user.position_live)
        # print('position_expired: ',user.position_expired)
        # print('total_invested_amount: ',user.total_invested_amount)
        # print('starting_amount: ',user.starting_amount)
        # print('current_total_invested_amount: ',user.current_total_invested_amount)
        # print('total_amount: ',user.total_amount)
        # print('total_profit_amount: ',user.total_profit_amount)
        # print('total_profit_return: ',user.total_profit_return)
        # populate_intraday_latest_price(ticker=[".CSI300"])
        # get_quote_yahoo("TCOM", use_symbol=True)
        # daily_hedge(currency="CNY")
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
        # update_index_price_from_dss(currency_code=["USD"])
        # print(user.client_user.all()[0].client.client_uid)
        # migrate_droid1.apply_async(queue='droid')
        # print(daily_hedge(currency="KRW"))
        # send_csv_hanwha(currency="KRW")
        # send_csv_hanwha(currency="CNY")
        # send_csv_hanwha(currency="HKD")
