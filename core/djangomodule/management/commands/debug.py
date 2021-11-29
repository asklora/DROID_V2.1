from ingestion.firestore_migration import firebase_ranking_update_random, firebase_user_update, firebase_universe_update
from bot.calculate_bot import populate_daily_profit, update_monthly_deposit, update_season, update_season_monthly
from django.core.management.base import BaseCommand
from core.services.tasks import daily_hedge_user
from core.services.exchange_services import market_task_checker
from core.services.notification import send_winner_email
# from core.services.healthcheck import check_firebase_schema, check_api, check_updater_schema, daily_health_check, check_market
from core.services.healthcheck.run import run_healthcheck
from datasource.rkd import RkdData
from core.universe.models import Universe
from core.bot.models import BotOptionType
import pandas as pd
from core.services.order_services import pending_order_checker
import asyncio



async def main():
    return await BotOptionType.objects.async_get(bot_id='UNO_ITM_05')

class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Process")
        # firebase_ranking_update_random()
        # ticker = list(Universe.objects.filter(currency_code__in=["HKD","USD"], 
        #             is_active=True).exclude(Error__contains='{').values_list('ticker',flat=True))
        # rkd = RkdData()
        # print(rkd.bulk_get_quote(['1797.HK',],df=True))
        # update_season_monthly()
        # update_monthly_deposit()
        # populate_daily_profit(user_id=[1846])
        # firebase_user_update(currency_code=["HKD"], update_firebase=False)
        # firebase_universe_update(currency_code=["HKD"])
        # daily_hedge_user(currency=["HKD"],ingest=True)