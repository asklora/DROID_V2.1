from ingestion.firestore_migration import firebase_user_update, firebase_universe_update
from bot.calculate_bot import populate_daily_profit
from django.core.management.base import BaseCommand
from core.services.tasks import daily_hedge_user
from datasource.rkd import RkdData
from core.universe.models import Universe
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Process")
        ticker = list(Universe.objects.filter(currency_code__in=["HKD","USD"], 
                    is_active=True).exclude(Error__contains='{').values_list('ticker',flat=True))
        rkd = RkdData()
        print(rkd.bulk_get_quote(['ALK',],df=True))
        # populate_daily_profit()
        # firebase_user_update()
        # firebase_universe_update(currency_code=["HKD"])
        # daily_hedge_user(currency=["HKD"],ingest=True)