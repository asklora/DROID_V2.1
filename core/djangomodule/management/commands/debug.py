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

        # dtypes = {'uid': 'text', 'trading_day': 'date', 'c2c_vol_0_21': 'double precision', 'c2c_vol_21_42': 'double precision', 'c2c_vol_42_63': 'double precision', 'c2c_vol_63_126': 'double precision', 'c2c_vol_126_252': 'double precision', 'c2c_vol_252_504': 'double precision', 'kurt_0_504': 'double precision', 'rs_vol_0_21': 'double precision', 'rs_vol_21_42': 'double precision', 'rs_vol_42_63': 'double precision', 'rs_vol_63_126': 'double precision', 'rs_vol_126_252': 'double precision', 'rs_vol_252_504': 'double precision', 'total_returns_0_1': 'double precision', 'total_returns_0_21': 'double precision', 'total_returns_0_63': 'double precision', 'total_returns_21_126': 'double precision', 'total_returns_21_231': 'double precision', 'vix_value': 'double precision', 'atm_volatility_spot': 'double precision', 'atm_volatility_one_year': 'double precision', 'atm_volatility_infinity': 'double precision', 'slope': 'double precision', 'deriv': 'double precision', 'slope_inf': 'double precision', 'deriv_inf': 'double precision', 'atm_volatility_spot_x': 'double precision', 'atm_volatility_one_year_x': 'double precision', 'atm_volatility_infinity_x': 'double precision', 'total_returns_0_63_x': 'double precision', 'total_returns_21_126_x': 'double precision', 'total_returns_0_21_x': 'double precision', 'total_returns_21_231_x': 'double precision', 'c2c_vol_0_21_x': 'double precision', 'c2c_vol_21_42_x': 'double precision', 'c2c_vol_42_63_x': 'double precision', 'c2c_vol_63_126_x': 'double precision', 'c2c_vol_126_252_x': 'double precision', 'c2c_vol_252_504_x': 'double precision', 'ticker': 'character varying'}
        # main_df = pd.read_pickle('bot_data.pkl')
        # # breakpoint()
        # print(main_df.dtypes)
        #
        # main_df = main_df.filter(list(dtypes.keys()))
        # float_col = ['total_returns_21_126', 'total_returns_21_231', 'total_returns_21_126_x', 'total_returns_21_231_x']
        # main_df[float_col] = main_df[float_col].astype(float)
        # print(main_df.shape)
        # print(main_df.dtypes)
        #
        # table_name = get_bot_data_table_name()
        # upsert_data_to_database(main_df, table_name, "uid", how="update", cpu_count=False, Text=True)
        # exit(200)

        # train_model(start_date='2010-01-01', end_date='2018-05-10')
        infer_history(start_date='2010-01-01', end_date='2012-01-01')
        infer_history(start_date='2012-01-01', end_date='2014-01-01')
        infer_history(start_date='2014-01-01', end_date='2016-01-01')
        infer_history(start_date='2016-01-01', end_date='2018-05-10')

