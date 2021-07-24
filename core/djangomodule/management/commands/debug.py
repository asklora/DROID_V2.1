from ingestion.data_from_rkd import update_currency_code_from_rkd
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Start Debug")
        ticker = ["MSFT.O", "AAPL.O"]
        update_currency_code_from_rkd(ticker=ticker)
        # tikers = [tick.ticker.ticker for tick in OrderPosition.objects.filter(ticker__currency_code='USD',is_live=True).distinct('ticker')]
        # for pos in tikers:
        #     pos.save()
        HKD_universe =  Universe.objects.filter(currency_code__in=['HKD'],is_active=True)
        for ticker in HKD_universe:
            ticker.ticker_symbol=symbol_hkd_fix(ticker.ticker_symbol)
            ticker.save()

        # print(HKD_universe)
        # rkd = RkdData()
        # now = datetime.now().date() - timedelta(days=1)
        # rkd.get_quote(tikers,save=True)
        # rkd.get_index_price('USD')
        # scrap_csi()
        # get_trkd_data_by_region('na')
        # user = User.objects.get(id=1)
        # print(user.check_password('pbkdf2_sha256$216000$SOyf9SnnXmzC$tpeNQM5F/AFhMMJNFnkZz='))
        # HKD_universe = [ ticker['ticker'] for ticker in Universe.objects.filter(ticker__in=['TSLA.O','JNJ']).exclude(ticker__in=['.SPX']).values('ticker')]
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
        # send_csv_hanwha(currency="USD",client_name="HANWHA",bot_tester=False,rehedge={
        #     'date':'2021-07-20',
        #     'types':'hedge'
        # })
        # send_csv_hanwha(currency="USD",client_name="HANWHA",bot_tester=True,rehedge={
        #     'date':'2021-07-20',
        #     'types':'hedge'
        # })
        # send_csv_hanwha(currency="CNY")
