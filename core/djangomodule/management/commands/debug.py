# from ingestion.firestore_migration import (
#     firebase_ranking_update_random,
#     firebase_user_update,
#     firebase_universe_update,
# )
# from bot.calculate_bot import (
#     populate_daily_profit,
#     update_monthly_deposit,
#     update_season_monthly,
# )
from django.core.management.base import BaseCommand

# from core.services.tasks import daily_hedge_user
# from core.services.exchange_services import market_task_checker
# from core.services.notification import send_winner_email

# from core.services.healthcheck.run import run_healthcheck

# from datasource.rkd import RkdData
# from core.universe.models import Universe
# import pandas as pd
# from core.services.order_services import pending_order_checker


class Command(BaseCommand):
    def handle(self, *args, **options):
        print("Process")
        # run_healthcheck.apply()
        # pending_order_checker(currency=["HKD"])
        # firebase_ranking_update_random()
        # ticker = list(
        #     Universe.objects.filter(
        #         currency_code__in=["HKD", "USD"],
        #         is_active=True,
        #     )
        #     .exclude(Error__contains="{")
        #     .values_list("ticker", flat=True)
        # )
        # rkd = RkdData()
        # rkd.bulk_get_quote(['XLNX.O','AAPL.O'],df=True,save=True)
        # update_season_monthly()
        # update_monthly_deposit()
        # populate_daily_profit(user_id=[1846])
        # firebase_user_update(currency_code=["HKD"], update_firebase=False)
        # firebase_universe_update(currency_code=["HKD"])
        # daily_hedge_user(currency=["HKD"],ingest=True)

        from main_executive import data_prep_history, train_model, infer_history 
        from general.date_process import backdate_by_year, str_to_date

        data_prep_history(start_date=str_to_date(backdate_by_year(13)))
        train_model(start_date=str_to_date(backdate_by_year(13)))
        infer_history(start_date=str_to_date(backdate_by_year(13)))
