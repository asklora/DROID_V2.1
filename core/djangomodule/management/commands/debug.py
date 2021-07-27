from ingestion.data_from_rkd import update_currency_code_from_rkd
from django.core.management.base import BaseCommand
from core.universe.models import ExchangeMarket,Universe
from core.Clients.models import UserClient
from core.orders.models import OrderPosition,PositionPerformance
from core.services.tasks import populate_client_top_stock_weekly,order_client_topstock,daily_hedge,send_csv_hanwha
from datasource.rkd import RkdData
from datetime import datetime


  

class Command(BaseCommand):
    def handle(self, *args, **options):
        serv =['bot_tester','bot_advisor']
        for a in serv:
            hanwha = [user["user"] for user in UserClient.objects.filter(client__client_name="HANWHA", 
                extra_data__service_type=a).values("user")]
            perf  = PositionPerformance.objects.filter(position_uid__ticker__currency_code='CNY',position_uid__user_id__in=hanwha,position_uid__is_live=True,order_uid__is_init=True,created__gte='2021-07-26 02:00:00.541000' ,created__lte='2021-07-26 02:00:29.541000')
            list_email = [str(p.order_uid.order_uid) for p in perf]
            print(list_email)
            if a == 'bot_tester':
                t = True
            else:
                t=False
            if perf:
                send_csv_hanwha(currency='CNY',client_name='HANWHA',new={'pos_list':list_email},bot_tester=t)

        # bbb =[]
        # bbb=[exc['mic'] for exc in ExchangeMarket.objects.filter(currency_code__in=['HKD']).values('mic')]
        # print(bbb)

        # print(HKD_universe)
#         HKD_universe = [ ticker['ticker'] for ticker in OrderPosition.objects.filter(ticker__currency_code__in=['HKD'], is_live=True).distinct('ticker').values('ticker')]
#         print(len(HKD_universe))

#         additional=['0857.HK',
# '1918.HK'
# ]
#         for ticker in additional:
#             if not ticker in HKD_universe:
#                 HKD_universe.append(ticker)
#             else:
#                 print(ticker)
#         print(len(HKD_universe))
#         rkd = RkdData()
#         now = datetime.now().date()
#         rkd.get_quote(HKD_universe, save=True,detail=f'hedge-{now}')
        # populate_client_top_stock_weekly(currency='HKD')
        # order_client_topstock(currency='HKD',bot_tester=False,repopulate={
        #     'date':'2021-07-26',
        #     'types':'populate'
        # })
        # order_client_topstock(currency='HKD',bot_tester=True,repopulate={
        #     'date':'2021-07-26',
        #     'types':'populate'
        # })
        # rkd.get_index_price('USD')
        # scrap_csi()
        # get_trkd_data_by_region('na')
        # user = User.objects.get(id=1)
        # print(user.check_password('pbkdf2_sha256$216000$SOyf9SnnXmzC$tpeNQM5F/AFhMMJNFnkZz='))
        # rkd = RkdStream(HKD_universe)
        # quotes = rkd.stream()
        # print(quotes)
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
        # daily_hedge(currency="HKD")
        # orders = [ids.order_uid for ids in Order.objects.filter(is_init=True)]
        # perf = PositionPerformance.objects.filter(
        #     position_uid__user_id__in=[108,
        #                                109,
        #                                110], created__gte=datetime.now().date(), position_uid__ticker__currency_code="KRW").order_by("created")
        # perf = perf.exclude(order_uid__in=orders)
        # print(perf.count())
        # populate_latest_price(currency_code="KRW")
        # update_index_price_from_dss(currency_code=["USD"])
        # print(user.client_user.all()[0].client.client_uid)
        # migrate_droid1.apply_async(queue='droid')
        # print(daily_hedge(currency="KRW"))
        # daily_hedge(currency="HKD",rehedge={
        #     'date':'2021-07-26',
        #     'types':'hedge'
        # })
        # daily_hedge(currency="KRW",client_name="HANWHA",bot_tester=True,rehedge={
        #     'date':'2021-07-26',
        #     'types':'hedge'
        # })
        # send_csv_hanwha(currency="CNY")

