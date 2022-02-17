# from main_executive import data_prep_history
from bot.option_file_classic import fill_bot_backtest_classic, populate_bot_classic_backtest
from bot.bot_labeler import populate_bot_labeler
from bot.statistic_classic import populate_classic_statistic
from bot.statistic_uno import populate_uno_statistic
from bot.statistic_ucdc import populate_ucdc_statistic
from bot.option_file_ucdc import fill_bot_backtest_ucdc, populate_bot_ucdc_backtest
from bot.option_file_uno import fill_bot_backtest_uno, populate_bot_uno_backtest
from bot.data_process import check_time_to_exp
from global_vars import folder_check, time_to_expiry, bots_list
from general.date_process import backdate_by_month, dateNow, droid_start_date, droid_start_date_buffer, str_to_date
from general.sql_query import get_active_universe, get_ticker_etf
from bot.main_file import populate_bot_data
from bot.final_model import populate_vol_infer

def data_prep_history(currency_code=None):
    print("{} : === DATA PREPERATION HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    print("Data preparation history started!")
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    ticker = get_active_universe(currency_code=currency_code)["ticker"].tolist()
    currency_code_to_etf = get_ticker_etf(currency_code=currency_code, active=True)
    universe_df = get_active_universe(ticker=currency_code_to_etf["etf_ticker"].to_list())
    universe_df = universe_df.loc[~universe_df["ticker"].isin(ticker)]
    ticker.extend(universe_df["ticker"].to_list())
    populate_bot_data(start_date=start_date, end_date=end_date, ticker=ticker, history=True)

def train_model(ticker=None, currency_code=None):
    ''' train bot model, details refer to populate_vol_infer() '''

    folder_check()
    print("{} : === VOLATILITY TRAIN MODEL STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_vol_infer(start_date, end_date, ticker=ticker, currency_code=currency_code, train_model=True)

def infer_history(currency_code=None):
    folder_check()
    print("{} : === VOLATILITY INFER HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    ticker = get_active_universe(currency_code=currency_code)["ticker"].tolist()
    populate_vol_infer(start_date, end_date, ticker=ticker, history=True)

def option_maker_history_uno(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UNO HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    if option_maker:
        populate_bot_uno_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, infer=infer, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_uno(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True, total_no_of_runs=total_no_of_runs, run_number=run_number)

def option_maker_history_ucdc(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UCDC HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    if option_maker:
        populate_bot_ucdc_backtest(ticker = ticker, currency_code=currency_code, start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, mod=mod, infer=infer, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_ucdc(start_date=start_date, end_date=end_date, ticker = ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True, total_no_of_runs=total_no_of_runs, run_number=run_number)

def option_maker_history_classic(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False):
    time_to_exp = check_time_to_exp(time_to_exp)
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    print("{} : === OPTION MAKER CLASSIC HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    if option_maker:
        populate_bot_classic_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_classic(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, mod=mod)

def bot_statistic_classic(ticker=None, currency_code=None, time_to_exp=None):
    print("{} : === BOT STATISTIC CLASSIC STARTED ===".format(dateNow()))
    time_to_exp = check_time_to_exp(time_to_exp)
    populate_classic_statistic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)


def bot_statistic_ucdc(ticker=None, currency_code=None, time_to_exp=None):
    print("{} : === BOT STATISTIC UCDC STARTED ===".format(dateNow()))
    time_to_exp = check_time_to_exp(time_to_exp)
    populate_ucdc_statistic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)

def bot_statistic_uno(ticker=None, currency_code=None, time_to_exp=None):
    print("{} : === BOT STATISTIC UNO STARTED ===".format(dateNow()))
    time_to_exp = check_time_to_exp(time_to_exp)
    populate_uno_statistic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)

def train_lebeler_model(ticker=None, currency_code=None, time_to_exp=time_to_expiry, bots_list=bots_list):
    folder_check()
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === BOT RANKING TRAIN MODEL STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, time_to_exp=time_to_exp, bot_labeler_train = True, bots_list=bots_list)

def bot_ranking_history(ticker=None, currency_code=None, mod=False, time_to_exp=time_to_expiry):
    folder_check()
    print("{} : === BOT RANKING HISTORY STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(backdate_by_month(6))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, time_to_exp=time_to_exp, currency_code=currency_code, mod=mod, history=True)

if __name__ == "__main__":
    # data_prep_history(currency_code=["USD", "HKD", "CNY"])
    # train_model(currency_code=["USD", "HKD", "CNY"])
    # infer_history(currency_code=["USD", "HKD", "CNY"])
    # option_maker_history_classic(currency_code=["USD", "HKD", "CNY"], option_maker=True, null_filler=True)
    # option_maker_history_uno(currency_code=["CNY"], option_maker=True, null_filler=False, infer=True)
    # option_maker_history_uno(currency_code=["USD"], option_maker=True, null_filler=False, infer=False)
    # option_maker_history_uno(currency_code=["HKD"], option_maker=True, null_filler=False, infer=True)
    # option_maker_history_ucdc(currency_code=["USD"], option_maker=True, null_filler=False, infer=False)
    # option_maker_history_ucdc(currency_code=["HKD"], option_maker=True, null_filler=False, infer=True)
    # option_maker_history_ucdc(currency_code=["CNY"], option_maker=True, null_filler=False, infer=True)

    # option_maker_history_uno(currency_code=["CNY"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_uno(currency_code=["USD"], option_maker=False, null_filler=True, infer=False, total_no_of_runs=2, run_number=0)
    # option_maker_history_uno(currency_code=["HKD"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_ucdc(currency_code=["USD"], option_maker=False, null_filler=True, infer=False, total_no_of_runs=2, run_number=0)
    # option_maker_history_ucdc(currency_code=["HKD"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=0)
    # option_maker_history_ucdc(currency_code=["CNY"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=0)

    # option_maker_history_uno(currency_code=["CNY"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    # option_maker_history_uno(currency_code=["USD"], option_maker=False, null_filler=True, infer=False, total_no_of_runs=2, run_number=1)
    # option_maker_history_uno(currency_code=["HKD"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    # option_maker_history_ucdc(currency_code=["USD"], option_maker=False, null_filler=True, infer=False, total_no_of_runs=2, run_number=1)
    # option_maker_history_ucdc(currency_code=["HKD"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    # option_maker_history_ucdc(currency_code=["CNY"], option_maker=False, null_filler=True, infer=True, total_no_of_runs=2, run_number=1)
    
    # bot_statistic_classic(currency_code=["USD", "HKD", "CNY"])
    # bot_statistic_uno(currency_code=["USD", "HKD", "CNY"])
    # bot_statistic_ucdc(currency_code=["USD", "HKD", "CNY"])
    # train_lebeler_model(currency_code=["USD", "HKD", "CNY"])
    # bot_ranking_history(currency_code=["USD", "HKD", "CNY"])
    pass
