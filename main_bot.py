from general.sql_query import get_active_universe
from general.sql_process import do_function
from main_executive import (
    data_prep_daily, 
    data_prep_history, 
    infer_daily, 
    infer_history, 
    option_maker_daily_ucdc, 
    option_maker_history_classic, 
    option_maker_history_uno, train_model)

if __name__ == "__main__":
    # do_function("data_vol_surface_update")
    # ticker = get_active_universe()["ticker"].tolist()
    # data_prep_daily(ticker=ticker)
    # infer_daily(ticker=ticker)
    # data_prep_history()
    # train_model()
    # infer_history()
    # option_maker_history_classic(currency_code="USD", option_maker=True, null_filler=False)
    # option_maker_history_classic(currency_code="EUR", option_maker=True, null_filler=False)
    option_maker_history_uno(currency_code="USD", option_maker=True, null_filler=False, infer=False)
    option_maker_history_uno(currency_code="EUR", option_maker=True, null_filler=False, infer=True)
    option_maker_daily_ucdc(currency_code="USD", option_maker=True, null_filler=False, infer=False)
    option_maker_daily_ucdc(currency_code="EUR", option_maker=True, null_filler=False, infer=True)