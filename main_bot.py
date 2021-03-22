from general.sql_query import get_active_universe
from general.sql_process import do_function
from main_executive import (
    bot_ranking_history, bot_statistic_classic, bot_statistic_ucdc, bot_statistic_uno, data_prep_daily, 
    data_prep_history, 
    infer_daily, 
    infer_history, 
    option_maker_daily_ucdc, 
    option_maker_history_classic, 
    option_maker_history_ucdc, 
    option_maker_history_uno, train_lebeler_model, train_model)

if __name__ == "__main__":
    print("Start Process")
    # train_lebeler_model()
    bot_ranking_history(ticker=None, currency_code=None)
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