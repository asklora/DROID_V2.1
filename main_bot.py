from general.sql_query import get_active_universe
from general.sql_process import do_function
from main_executive import (
    bot_statistic_classic, bot_statistic_ucdc, bot_statistic_uno, data_prep_daily, 
    data_prep_history, 
    infer_daily, 
    infer_history, 
    option_maker_daily_ucdc, 
    option_maker_history_classic, 
    option_maker_history_ucdc, 
    option_maker_history_uno, train_model)

if __name__ == "__main__":
    print("Start Process")
    bot_statistic_classic()
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
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=0)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=1)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=2)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=3)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=4)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=5)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=6)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=7)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=8)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=9)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=10)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=11)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=12)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=13)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=14)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=15)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=16)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=17)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=18)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=19)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=20)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=21)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=22)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=23)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=24)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=25)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=26)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=27)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=28)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=29)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=30)
    # option_maker_history_uno(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=31)

    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=0)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=1)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=2)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=3)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=4)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=5)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=6)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=7)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=8)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=9)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=10)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=11)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=12)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=13)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=14)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=15)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=16)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=17)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=18)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=19)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=20)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=21)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=22)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=23)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=24)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=25)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=26)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=27)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=28)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=29)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=30)
    # option_maker_history_uno(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=31)

    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=0)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=1)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=2)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=3)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=4)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=5)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=6)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=7)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=8)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=9)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=10)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=11)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=12)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=13)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=14)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=15)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=16)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=17)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=18)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=19)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=20)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=21)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=22)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=23)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=24)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=25)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=26)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=27)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=28)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=29)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=30)
    # option_maker_history_ucdc(currency_code="USD", null_filler=True, infer=False, total_no_of_runs=32, run_number=31)

    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=1)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=2)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=3)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=4)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=5)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=6)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=7)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=8)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=9)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=10)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=11)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=12)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=13)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=14)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=15)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=16)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=17)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=18)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=19)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=20)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=21)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=22)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=23)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=24)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=25)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=26)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=27)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=28)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=29)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=30)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=31)
    # option_maker_history_ucdc(currency_code="EUR", null_filler=True, infer=True, total_no_of_runs=32, run_number=32)
    