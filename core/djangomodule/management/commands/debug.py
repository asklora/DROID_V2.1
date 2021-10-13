from django.core.management.base import BaseCommand
class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Process")
        # populate_daily_profit()
        # firebase_user_update()