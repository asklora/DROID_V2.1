from portfolio.daily_hedge_uno import uno_position_check
from bot.calculate_bot import populate_daily_profit
from ingestion.mongo_migration import firebase_user_update
from django.core.management.base import BaseCommand
#debug
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Something")
        
        uno_position_check("4cad83492f4749549a21412925560f4b", to_date=None, tac=False, hedge=False, latest=True)
        populate_daily_profit(user_id=[689])
        firebase_user_update(user_id=[689])
        