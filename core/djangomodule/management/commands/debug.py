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
        
        # uno_position_check("4cad83492f4749549a21412925560f4b", to_date=None, tac=False, hedge=False, latest=True)
        # populate_daily_profit()
        # firebase_user_update()

        convert = ConvertMoney()
        print(convert.convert_money(100, "USD", "HKD"))
        print(convert.convert_money(100, "USD", "USD"))
        print(convert.convert_money(100, "HKD", "USD"))
        print(convert.convert_money(100, "KRW", "USD"))
        print(convert.convert_money(100, "USD", "KRW"))