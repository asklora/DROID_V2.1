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
    # data_prep_daily()
    # do_function("latest_bot_data")
    # train_model()
    # infer_daily()
    # do_function("latest_vol")

    # data_prep_history()
    # do_function("latest_bot_data")
    # train_model()
    # infer_history()
    # do_function("latest_vol")

    # data_prep_history()
    # do_function("latest_bot_data")
    # train_model()
    # infer_history()
    # do_function("latest_vol")

    # populate_latest_bot_update(currency_code=["KRW"])
    # populate_latest_bot_update(currency_code=["HKD"])
    # populate_latest_bot_update(currency_code=["CNY"])
    # populate_latest_bot_update(currency_code=["USD"])
    # populate_latest_bot_update(currency_code=["EUR"])

    # option_maker_history_uno(currency_code=["KRW"], option_maker=True, infer=True)
    # option_maker_history_uno(currency_code=["HKD"], option_maker=True, infer=True)
    # option_maker_history_uno(currency_code=["CNY"], option_maker=True, infer=True)
    # option_maker_daily_uno(currency_code=["USD"], option_maker=True, infer=False)
    # option_maker_daily_uno(currency_code=["EUR"], option_maker=True, infer=True)
    # option_maker_uno_check_new_ticker(currency_code=["EUR"], option_maker=True, infer=True)
    # option_maker_uno_check_new_ticker(currency_code=["USD"], option_maker=True, infer=False)
    
    # option_maker_history_ucdc(currency_code=["KRW"], option_maker=True, infer=True)
    # option_maker_history_ucdc(currency_code=["HKD"], option_maker=True, infer=True)
    # option_maker_history_ucdc(currency_code=["CNY"], option_maker=True, infer=True)
    # option_maker_daily_ucdc(currency_code=["USD"], option_maker=True, infer=False)
    # option_maker_daily_ucdc(currency_code=["EUR"], option_maker=True, infer=True)
    # option_maker_ucdc_check_new_ticker(currency_code=["EUR"], option_maker=True, infer=True)
    # option_maker_ucdc_check_new_ticker(currency_code=["USD"], option_maker=True, infer=False)

    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=0)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=1)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=2)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=3)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=4)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=5)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=6)
    # option_maker_history_uno(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=8, run_number=7)

    # option_maker_history_uno(currency_code=["HKD"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)
    # option_maker_history_uno(currency_code=["CNY"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)
    # option_maker_daily_uno(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=1, run_number=0)
    # option_maker_daily_uno(currency_code=["EUR"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)

    # option_maker_history_ucdc(currency_code=["KRW"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)
    # option_maker_history_ucdc(currency_code=["HKD"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)
    # option_maker_history_ucdc(currency_code=["CNY"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)
    # option_maker_daily_ucdc(currency_code=["USD"], null_filler=True, infer=False, total_no_of_runs=1, run_number=0)
    # option_maker_daily_ucdc(currency_code=["EUR"], null_filler=True, infer=True, total_no_of_runs=1, run_number=0)

    # option_maker_history_classic(currency_code=["KRW"], option_maker=True, null_filler=True)
    # option_maker_history_classic(currency_code=["HKD"], option_maker=True, null_filler=True)
    # option_maker_history_classic(currency_code=["CNY"], option_maker=True, null_filler=True)
    # option_maker_history_classic(currency_code=["USD"], option_maker=True, null_filler=True)
    # daily_classic(currency_code=["EUR"], option_maker=True, null_filler=True)
    # bot_statistic_classic()

    # do_function("bot_backtest_updates")
    # train_lebeler_model()
    # bot_ranking_history()
    # do_function("latest_bot_update")
    # bot_statistic_ucdc()
    # bot_statistic_uno()


    # daily_classic(currency_code=["USD"], option_maker=True, null_filler=True)
    # daily_classic(currency_code=["EUR"], option_maker=True, null_filler=True)
    # daily_uno_ucdc(currency_code=["EUR"], infer=True, option_maker=True, null_filler=True)
    
    # train_lebeler_model()
    # populate_latest_bot_update(currency_code="USD")
    # bot_ranking_history(ticker=None, currency_code=None)
    # bot_statistic_classic()
    # bot_statistic_ucdc(currency_code="EUR")
    # bot_statistic_uno(currency_code="EUR")
    # do_function("data_vol_surface_update")
    # ticker = get_active_universe()["ticker"].tolist()
    # data_prep_daily(ticker=ticker)
    # infer_daily(ticker=ticker)
    # data_prep_history()
    # train_model()
    # infer_history()
    # option_maker_history_classic(currency_code="USD", option_maker=True, null_filler=False)
    # option_maker_history_classic(currency_code="EUR", option_maker=True, null_filler=False)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=1, run_number=0)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=1, run_number=0)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=1, run_number=0)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=1, run_number=1)