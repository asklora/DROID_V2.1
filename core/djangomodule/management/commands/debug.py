from core.universe.models import Currency,ExchangeMarket,Universe
from core.user.convert import ConvertMoney
from core.orders.models import Order, OrderPosition
from core.user.models import User
from portfolio.daily_hedge_uno import uno_position_check
from bot.calculate_bot import populate_daily_profit, update_monthly_deposit
from ingestion.firestore_migration import firebase_user_update
from django.core.management.base import BaseCommand
from core.services.tasks import daily_hedge
from core.services.order_services import pending_order_checker
from datasource.rkd import RkdData,RkdStream
from core.services.tasks import daily_hedge_user
from django.utils import timezone
from django.db.models import Q
#debug
class Command(BaseCommand):
    def handle(self, *args, **options):
        ticks=['AAPL.O',
        'ABBV.K',
        'ABC',
        'ABT',
        'ACN',
        'DXCM.O',
        'ECL',
        'EIX',
        'ENPH.O',
        'EQR',
        'ETR',
        'GOOGL.O',
        'HBAN.O',
        'HBI',]

        # open_market=list(ExchangeMarket.objects.filter(currency_code__in=["HKD","USD"],
        #     group="Core",is_open=True).values_list("currency_code",flat=True))
        # univ =list(Universe.objects.filter(currency_code__in=open_market, 
        # is_active=True).exclude(entity_type='index').values_list('ticker',flat=True))
        rkd = RkdData()
        res=rkd.bulk_get_quote(ticks,df=True)
        print(res)
        res.to_csv('example_usd.csv',index=False)
        # pending_order_checker()
        # daily_hedge(currency="HKD")
        # print("Something")
        # ticker_data =list(Universe.objects.filter(currency_code__in=["HKD","USD"], 
        #         is_active=True).exclude(Error__contains='{').values_list('ticker',flat=True))
        # print(len(ticker_data))
        # rkd = RkdStream()
        # rkd.stream_quote()
        # data =rkd.bulk_get_quote(ticker_data,save=True)
        # data.to_csv('usd_rkd_quote.csv',index=False)
        # print(data)
        # data = data.set_index('ticker')
        # records = data.to_dict("index")
        # rkd.update_rtdb(records)
        # print("Something")
        # user = list(User.objects.filter(current_status="verified").values_list("id",flat=True))
        # print(len(univ))
        # order = Order.objects.create(
        #     amount=10000,
        #     bot_id="CLASSIC_classic_008333",
        #     # bot_id="STOCK_stock_0",
        #     # bot_id="UCDC_ATM_008333",
        #     # bot_id="UNO_ITM_008333",
        #     # bot_id="UNO_OTM_008333",
        #     created = timezone.now(),
        #     margin=2,
        #     order_type="apps",  # to differentiate itself from FELS's orders
        #     price=299.35,
        #     side="buy",
        #     ticker_id="MSFT.O",
        #     user_id_id=517
        # )
        # order = Order.objects.get(user_id = 517)
        # order.status = 'filled'
        # order.save()
        # order.status = 'pending'
        # order.save()
        # users = [user['id'] for user in User.objects.filter(is_superuser=False,current_status="verified").values('id')]
        # print(users)
        # populate_daily_profit()
        # update_monthly_deposit(user_id = [229])
        # uno_position_check("4cad83492f4749549a21412925560f4b", to_date=None, tac=False, hedge=False, latest=True)
        # populate_daily_profit()
        # firebase_user_update(user_id=users)
        # firebase_user_update()

        # from_curr = Currency.objects.get(currency_code="EUR")
        # to_curr = Currency.objects.get(currency_code="USD")
        # convert = ConvertMoney(from_curr, to_curr)
        # print(convert.convert(100))

        # from_curr = Currency.objects.get(currency_code="USD")
        # to_curr = Currency.objects.get(currency_code="EUR")
        # convert = ConvertMoney(from_curr, to_curr)
        # print(convert.convert(100))