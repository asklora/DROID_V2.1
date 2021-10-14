from ingestion.firestore_migration import firebase_user_update
from bot.calculate_bot import populate_daily_profit
from django.core.management.base import BaseCommand
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Process")
        populate_daily_profit()
        firebase_user_update()