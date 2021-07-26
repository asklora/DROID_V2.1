from ingestion.data_from_rkd import update_currency_code_from_rkd
from django.core.management.base import BaseCommand
from core.universe.models import ExchangeMarket,Universe
from core.orders.models import OrderPosition
from core.services.tasks import populate_client_top_stock_weekly,order_client_topstock,daily_hedge,send_csv_hanwha
from datasource.rkd import RkdData
from datetime import datetime





class Command(BaseCommand):
    def handle(self, *args, **options):
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
        # send_csv_hanwha(currency='HKD',client_name='HANWHA',new={'pos_list':[
        #             '843dba65-d8bb-4a63-8d80-cafe043c542d',
        #             'ebaea083-fbfb-4e3d-8795-c07b4cfd6fe7',
        #             '4f499f7a-7a88-4729-9b18-c4d066970c60',
        #             '2caa6b9c-7c98-495b-9f31-b964dd2acc2a',
        #             'ab964ef5-2d6c-41e9-a78b-4ed5b05e73fb',
        #             'b53a8a2c-3e80-4831-9591-7e81573c8b57',
        #             '003736ce-5689-4667-a99d-84a6e935ce63',
        #             '48523fa5-71d9-412b-b052-ac2108b83175',
        #             'bf5aee7b-1b08-4c33-a626-85c4ef8e25d0',
        #             '1e72a8f3-6c27-4935-b58a-19f595a81b17',
        #             '00551448-35fa-497b-93d9-67f3b2e6fa55',
        #             '56348ea4-165e-4dc4-8fd2-ed81b7653ad2',
        #             'a0954c7c-5af7-4941-a781-3a81e75e92b9',
        #             'e5a441de-ba2b-49bd-9f97-af908dcce73e',
        #             '54ad6f11-04f5-4d52-bbdd-cee20b5074c1',
        #             '60a3047a-cdb0-412c-9a13-e75ebeb2bfc2',
        #             '5b94eb58-8b5d-4353-a692-2682bd0cc6f7',
        #             '7177ade2-a916-4b34-88f6-fc92291e005f',
        #             '4e3842eb-795a-4ae7-b1d0-d84bb2985aa2',
        #             'a6c9fa14-9107-444d-95a6-1e08d06f3659',
        #             'd739cbb9-5d32-4a32-8724-7211a8b15290',
        #             'af20cf7f-350a-454f-87f5-b390df4106e6',
        #             '67099c50-a013-4d40-8818-2fe49733f0fe',
        #             'b1a52cea-2ae1-4e09-8bf7-7aa95a9d9f5a',
        #             '61f004d0-95a4-45cb-89f5-8dc8f3bdd342',
        #             '88299e0b-8c33-4307-9067-10c977b3f848',
        #             '8a0e5e04-8769-4e92-b34a-fd6fa2ce27ef',
        # ]})
        # update_index_price_from_dss(currency_code=["USD"])
        # print(user.client_user.all()[0].client.client_uid)
        # migrate_droid1.apply_async(queue='droid')
        # print(daily_hedge(currency="KRW"))
        daily_hedge(currency="HKD",rehedge={
            'date':'2021-07-26',
            'types':'hedge'
        })
        # daily_hedge(currency="KRW",client_name="HANWHA",bot_tester=True,rehedge={
        #     'date':'2021-07-26',
        #     'types':'hedge'
        # })
        # send_csv_hanwha(currency="CNY")

