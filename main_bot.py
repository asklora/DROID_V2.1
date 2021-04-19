from ingestion.master_tac import master_tac_update
from ingestion.master_multiple import master_multiple_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from ingestion.master_data import update_quandl_orats_from_quandl
from bot.calculate_latest_bot import populate_latest_bot_update
from general.sql_query import get_active_universe
from general.sql_process import do_function
from main_executive import (
    bot_ranking_history, bot_statistic_classic, bot_statistic_ucdc, bot_statistic_uno, daily_classic, daily_uno_ucdc, data_prep_daily, 
    data_prep_history, 
    infer_daily, 
    infer_history, 
    option_maker_daily_ucdc, option_maker_daily_uno, 
    option_maker_history_classic, 
    option_maker_history_ucdc, 
    option_maker_history_uno, option_maker_ucdc_check_new_ticker, option_maker_uno_check_new_ticker, train_lebeler_model, train_model)

if __name__ == "__main__":
    print("Start Process")

    # update_quandl_orats_from_quandl()
    # do_function("data_vol_surface_update")

    # data_prep_daily(currency_code=["KRW"])
    # data_prep_daily(currency_code=["HKD"])
    # data_prep_daily(currency_code=["CNY"])
    # data_prep_daily(currency_code=["USD"])
    # data_prep_daily(currency_code=["EUR"])
    # do_function("latest_bot_data")

    # infer_daily(currency_code=["KRW"])
    # infer_daily(currency_code=["HKD"])
    # infer_daily(currency_code=["CNY"])
    # infer_daily(currency_code=["USD"])
    # infer_daily(currency_code=["EUR"])
    # do_function("latest_vol")

    # do_function("master_ohlcvtr_update")
    # master_ohlctr_update()
    # master_tac_update()
    # master_multiple_update()

    # train_model()
    # infer_history()

    # populate_latest_bot_update(currency_code=["KRW"])
    # populate_latest_bot_update(currency_code=["HKD"])
    # populate_latest_bot_update(currency_code=["CNY"])
    # populate_latest_bot_update(currency_code=["USD"])
    # populate_latest_bot_update(currency_code=["EUR"])

    # option_maker_daily_uno(currency_code=["KRW"], option_maker=True, infer=True)
    # option_maker_daily_uno(currency_code=["HKD"], option_maker=True, infer=True)
    # option_maker_daily_uno(currency_code=["CNY"], option_maker=True, infer=True)
    # option_maker_daily_uno(currency_code=["USD"], option_maker=True, infer=False)
    # option_maker_daily_uno(currency_code=["EUR"], option_maker=True, infer=True)
    
    # option_maker_daily_ucdc(currency_code=["KRW"], option_maker=True, infer=True)
    # option_maker_daily_ucdc(currency_code=["HKD"], option_maker=True, infer=True)
    # option_maker_daily_ucdc(currency_code=["CNY"], option_maker=True, infer=True)
    # option_maker_daily_ucdc(currency_code=["USD"], option_maker=True, infer=False)
    # option_maker_daily_ucdc(currency_code=["EUR"], option_maker=True, infer=True)

    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=0)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=1)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=2)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=3)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=4)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=5)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=6)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=7)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=8)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=9)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=10)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=11)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=12)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=13)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=14)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=15)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=16)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=17)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=18)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=19)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=20)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=21)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=22)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=23)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=24)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=25)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=26)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=27)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=28)
    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=29)

    # option_maker_daily_classic(currency_code=["KRW"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["HKD"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["CNY"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["USD"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["EUR"], option_maker=True, null_filler=True)

    # do_function("bot_backtest_updates")
    # train_lebeler_model()
    # bot_ranking_history()
    # do_function("latest_bot_update")
    # bot_statistic_ucdc()
    # bot_statistic_uno()
    # bot_statistic_classic()