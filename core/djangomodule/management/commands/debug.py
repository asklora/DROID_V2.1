from core.universe.models import Currency
from core.user.convert import ConvertMoney
from core.orders.models import OrderPosition
from core.user.models import User
from portfolio.daily_hedge_uno import uno_position_check
from bot.calculate_bot import populate_daily_profit, update_monthly_deposit
from ingestion.firestore_migration import firebase_user_update
from django.core.management.base import BaseCommand
from core.services.tasks import daily_hedge
#debug
class Command(BaseCommand):
    def handle(self, *args, **options):
        # daily_hedge(currency="HKD")
        # print("Something")
        users = [user['id'] for user in User.objects.filter(is_superuser=False,current_status="verified").values('id')]
        # print(users)
        populate_daily_profit()
        # update_monthly_deposit(user_id = [229])
        # uno_position_check("4cad83492f4749549a21412925560f4b", to_date=None, tac=False, hedge=False, latest=True)
        # populate_daily_profit()
        firebase_user_update(user_id=users)

        # from_curr = Currency.objects.get(currency_code="HKD")
        # to_curr = Currency.objects.get(currency_code="USD")
        # convert = ConvertMoney(from_curr, to_curr)
        # print(convert.convert(100))
        # print(convert.to_hkd(100))
        # print(convert.to_usd(100))
        # print(convert.to_krw(100))

        # from_curr = Currency.objects.get(currency_code="USD")
        # to_curr = Currency.objects.get(currency_code="HKD")
        # convert = ConvertMoney(from_curr, to_curr)
        # print(convert.convert(100))
        # print(convert.to_hkd(100))
        # print(convert.to_usd(100))
        # print(convert.to_krw(100))
        # print(convert.to_eur(100))