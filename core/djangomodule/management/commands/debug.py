from core.universe.models import Currency
from core.user.convert import ConvertMoney
from core.orders.models import OrderPosition
from core.user.models import User
from portfolio.daily_hedge_uno import uno_position_check
from bot.calculate_bot import populate_daily_profit
from ingestion.mongo_migration import firebase_user_update
from django.core.management.base import BaseCommand
#debug
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Something")
        # populate_daily_profit()
        # uno_position_check("4cad83492f4749549a21412925560f4b", to_date=None, tac=False, hedge=False, latest=True)
        # populate_daily_profit()
        # firebase_user_update()

        from_curr = Currency.objects.get(currency_code="HKD")
        to_curr = Currency.objects.get(currency_code="USD")
        convert = ConvertMoney(from_curr, to_curr)
        print(convert.convert(100))
        print(convert.to_hkd(100))
        print(convert.to_usd(100))
        print(convert.to_krw(100))

        from_curr = Currency.objects.get(currency_code="USD")
        to_curr = Currency.objects.get(currency_code="HKD")
        convert = ConvertMoney(from_curr, to_curr)
        print(convert.convert(100))
        print(convert.to_hkd(100))
        print(convert.to_usd(100))
        print(convert.to_krw(100))
        print(convert.to_eur(100))