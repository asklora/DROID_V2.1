from bot.calculate_bot import populate_daily_profit
from ingestion.mongo_migration import firebase_user_update
from django.core.management.base import BaseCommand
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Something")
        # populate_daily_profit()
        firebase_user_update(user_id=[689])