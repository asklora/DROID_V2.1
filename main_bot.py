from bot.preprocess import dividend_daily_update, interest_daily_update
from ingestion.master_tac import master_tac_update
from ingestion.master_multiple import master_multiple_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from ingestion.master_data import update_quandl_orats_from_quandl
from bot.calculate_latest_bot import populate_latest_bot_update
from general.sql_query import get_active_universe
from general.sql_process import do_function
from main_executive import (
    bot_ranking_daily, bot_ranking_history, bot_statistic_classic, bot_statistic_ucdc, bot_statistic_uno, daily_classic, daily_uno_ucdc, data_prep_daily,
    data_prep_history,
    infer_daily,
    infer_history,
    option_maker_daily_ucdc, option_maker_daily_uno,
    option_maker_history_classic,
    option_maker_history_ucdc,
    option_maker_history_uno, option_maker_ucdc_check_new_ticker, option_maker_uno_check_new_ticker, train_lebeler_model, train_model)

if __name__ == "__main__":
    print("Start Process")
    # bot_ranking_daily(currency_code=["USD"], mod=False)
    # data_prep_history(currency_code=["KRW"])
    # data_prep_history(currency_code=["HKD"])
    # data_prep_history(currency_code=["CNY"])
    # data_prep_history(currency_code=["USD"])
    # data_prep_history(currency_code=["EUR"])

    # daily_uno_ucdc(currency_code=["KRW"], infer=True)
    # daily_uno_ucdc(currency_code=["HKD"], infer=True)
    # daily_uno_ucdc(currency_code=["CNY"], infer=True)
    # daily_uno_ucdc(currency_code=["USD"], infer=False)
    # daily_uno_ucdc(currency_code=["EUR"], infer=True)

    # daily_classic()

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

    # option_maker_history_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=120, run_number=40)

    # option_maker_daily_classic(currency_code=["KRW"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["HKD"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["CNY"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["USD"], option_maker=True, null_filler=True)
    # option_maker_daily_classic(currency_code=["EUR"], option_maker=True, null_filler=True)

    # train_model()
    # do_function("bot_backtest_updates")
    # train_lebeler_model(time_to_exp=time_to_expiry, bots_list=bots_list)
    # time_to_expiry = [0.03846, 0.07692, 0.08333, 0.15384, 0.16666, 0.25, 0.5]
    # time_to_expiry = [0.03846, 0.07692, 0.08333, 0.15384, 0.16666, 0.25, 0.5]
    # bot_ranking_history(time_to_exp=time_to_expiry)
    # do_function("latest_bot_update")
    # bot_statistic_ucdc()
    # bot_statistic_uno()
    # bot_statistic_classic()
    # interest_daily_update()
    # dividend_daily_update()

    # data_prep_history(currency_code=["TWD"])
    # data_prep_history(currency_code=["JPY"])
    # data_prep_history(currency_code=["GBP"])
    # data_prep_history(currency_code=["SGD"])
    # do_function("latest_bot_data")
    # train_model()
    # do_function("latest_vol")
    # infer_history(currency_code=["TWD"])
    # infer_history(currency_code=["JPY"])
    # infer_history(currency_code=["GBP"])
    # infer_history(currency_code=["SGD"])

    # populate_latest_bot_update(currency_code=["TWD"])
    # populate_latest_bot_update(currency_code=["JPY"])
    # populate_latest_bot_update(currency_code=["GBP"])
    # populate_latest_bot_update(currency_code=["SGD"])

    # option_maker_history_uno(currency_code=["TWD"], option_maker=True, infer=True)
    # option_maker_history_uno(currency_code=["JPY"], option_maker=True, infer=True)
    # option_maker_history_uno(currency_code=["GBP"], option_maker=True, infer=True)
    # option_maker_history_uno(currency_code=["SGD"], option_maker=True, infer=True)

    # option_maker_history_ucdc(currency_code=["TWD"], option_maker=True, infer=True)
    # option_maker_history_ucdc(currency_code=["JPY"], option_maker=True, infer=True)
    # option_maker_history_ucdc(currency_code=["GBP"], option_maker=True, infer=True)
    # option_maker_history_ucdc(currency_code=["SGD"], option_maker=True, infer=True)

    # option_maker_history_uno(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=5, run_number=0)
    # option_maker_history_uno(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=5, run_number=1)
    # option_maker_history_uno(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=5, run_number=2)
    # option_maker_history_uno(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=5, run_number=3)
    # option_maker_history_uno(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=5, run_number=4)
    # option_maker_history_uno(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=5, run_number=0)
    # option_maker_history_uno(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=5, run_number=1)
    # option_maker_history_uno(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=5, run_number=2)
    # option_maker_history_uno(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=5, run_number=3)
    # option_maker_history_uno(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=5, run_number=4)
    # option_maker_history_uno(currency_code=["GBP"], null_filler=True, infer=True, total_no_of_runs=4, run_number=0)
    # option_maker_history_uno(currency_code=["GBP"], null_filler=True, infer=True, total_no_of_runs=4, run_number=1)
    # option_maker_history_uno(currency_code=["GBP"], null_filler=True, infer=True, total_no_of_runs=4, run_number=2)
    # option_maker_history_uno(currency_code=["GBP"], null_filler=True, infer=True, total_no_of_runs=4, run_number=3)
    # option_maker_history_uno(currency_code=["SGD"], null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_uno(currency_code=["SGD"], null_filler=True, infer=True, total_no_of_runs=2, run_number=1)

    # option_maker_history_ucdc(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_ucdc(currency_code=["TWD"], null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    # option_maker_history_ucdc(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_ucdc(currency_code=["JPY"], null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    # option_maker_history_ucdc(currency_code=["GBP"], null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_ucdc(currency_code=["GBP"], null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    # option_maker_history_ucdc(currency_code=["SGD"], null_filler=True, infer=True)

    # option_maker_history_classic(currency_code=["TWD"], option_maker=True, null_filler=True)
    # option_maker_history_classic(currency_code=["JPY"], option_maker=True, null_filler=True)
    # option_maker_history_classic(currency_code=["GBP"], option_maker=True, null_filler=True)
    # option_maker_history_classic(currency_code=["SGD"], option_maker=True, null_filler=True)
