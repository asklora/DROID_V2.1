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
        import pandas as pd
        from general.table_name import get_bot_data_table_name
        from general.sql_output import upsert_data_to_database
        # data_prep_history(start_date=str_to_date(backdate_by_year(13)))

        main_df = pd.read_pickle('bot_data.pkl')
        print(main_df.dtypes)

        main_df = main_df.drop(columns=['2009-04-07_x', '2009-04-07_y', '2009-07-02', '2009-07-31', '2009-12-25'])
        float_col = ['total_returns_21_126', 'total_returns_21_231', 'total_returns_21_126_x', 'total_returns_21_231_x']
        main_df[float_col] = main_df[float_col].astype(float)
        print(main_df.shape)
        print(main_df.dtypes)

        table_name = get_bot_data_table_name()
        upsert_data_to_database(main_df, table_name, "uid", how="update", cpu_count=False, Text=True)
        exit(200)

        train_model(start_date=str_to_date(backdate_by_year(13)))
        infer_history(start_date=str_to_date(backdate_by_year(13)))
