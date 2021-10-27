from ingestion.firestore_migration import firebase_user_update, firebase_universe_update
from bot.calculate_bot import populate_daily_profit
from django.core.management.base import BaseCommand
from core.services.tasks import daily_hedge_user
from datasource.rkd import RkdData
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Process")
        rkd = RkdData()
        print(rkd.get_quote(['MSFT.O','0586.HK'], df=True,save=True))
        # populate_daily_profit()
        # firebase_user_update()
        # firebase_universe_update(currency_code=["HKD"])
        # daily_hedge_user(currency=["HKD"],ingest=True)