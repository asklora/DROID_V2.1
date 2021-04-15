from bot.calculate_latest_bot import populate_latest_bot_update
from bot.bot_labeler import populate_bot_labeler
from bot.final_model import populate_vol_infer
from bot.main_file import populate_bot_data
from general.sql_process import do_function
from general.sql_query import get_active_universe
from bot.data_download import (
    get_backtest_latest_date, 
    get_bot_data_latest_date, 
    get_new_ticker_from_bot_backtest, 
     get_new_tickers_from_bot_data, 
     get_volatility_latest_date)
from general.slack import report_to_slack
from general.date_process import dateNow, droid_start_date_buffer, str_to_date, droid_start_date
from bot.option_file_classic import fill_bot_backtest_classic, populate_bot_classic_backtest
from bot.option_file_ucdc import fill_bot_backtest_ucdc, populate_bot_ucdc_backtest
from bot.option_file_uno import fill_bot_backtest_uno, populate_bot_uno_backtest
from bot.statistic_classic import populate_classic_statistic
from bot.statistic_ucdc import populate_ucdc_statistic
from bot.statistic_uno import populate_uno_statistic
from bot.data_process import check_bot_list, check_time_to_exp
from global_vars import folder_check, time_to_expiry

def training(ticker=None, currency_code=None):
    train_model(ticker=ticker, currency_code=currency_code)
    train_lebeler_model(ticker=ticker, currency_code=currency_code)

# follow currency schedule
def daily_uno_ucdc(ticker=None, currency_code=None, time_to_exp=time_to_expiry, infer=True, option_maker=True, null_filler=True, mod=False, total_no_of_runs=1, run_number=0):
    #Data Preparation
    data_prep_daily(ticker=ticker, currency_code=currency_code)
    data_prep_check_new_ticker(ticker=ticker, currency_code=currency_code)
    do_function("latest_bot_data")
    #Populate Volatility Infer
    if(infer):
        infer_daily(ticker=ticker, currency_code=currency_code)
        do_function("latest_vol")
    #Latest Bot Update Populate
    populate_latest_bot_update(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)
    #Option Maker & Null Filler UNO
    option_maker_uno_check_new_ticker(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, option_maker=option_maker, null_filler=null_filler, infer=infer, total_no_of_runs=total_no_of_runs, run_number=run_number)
    option_maker_daily_uno(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, option_maker=option_maker, null_filler=null_filler, infer=infer, total_no_of_runs=total_no_of_runs, run_number=run_number)
    #Option Maker & Null Filler UCDC
    option_maker_ucdc_check_new_ticker(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, option_maker=option_maker, null_filler=null_filler, infer=infer, total_no_of_runs=total_no_of_runs, run_number=run_number)
    option_maker_daily_ucdc(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, option_maker=option_maker, null_filler=null_filler, infer=infer, total_no_of_runs=total_no_of_runs, run_number=run_number)
    #Update Bot Backtest
    do_function("bot_backtest_updates")
    #Populate Bot Ranking
    bot_ranking_daily(ticker=ticker, currency_code=currency_code, mod=mod)
    #Update Bot Ranking to Latest Bot Update
    do_function("latest_bot_update")
    if(type(currency_code) == list):
        if any("USD" in s for s in currency_code):
            bot_statistic_ucdc(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)
            bot_statistic_uno(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)

# follow currency schedule
def daily_classic(ticker=None, currency_code=None, time_to_exp=time_to_expiry, mod=False, option_maker=True, null_filler=True):
    option_maker_classic_check_new_ticker(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, option_maker=option_maker, null_filler=null_filler)
    option_maker_daily_classic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, option_maker=option_maker, null_filler=null_filler)
    bot_ranking_daily()
    bot_statistic_classic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)


# ************************************************************************************************************************************************************************************
# *************************** MODEL TRAINING *****************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def train_lebeler_model(ticker=None, currency_code=None):
    folder_check()
    time_to_exp = check_time_to_exp(time_to_expiry)
    print("{} : === BOT RANKING TRAIN MODEL STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, time_to_exp=time_to_exp, bot_labeler_train = True)
    print("{} : === BOT RANKING TRAIN MODEL COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === BOT RANKING TRAIN MODEL COMPLETED ===".format(dateNow()))

def train_model(ticker=None, currency_code=None):
    folder_check()
    print("{} : === VOLATILITY TRAIN MODEL STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_vol_infer(start_date, end_date, ticker=ticker, currency_code=currency_code, train_model=True)
    print("{} : === VOLATILITY TRAIN MODEL COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === VOLATILITY TRAIN MODEL COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** DATA PREPARATION ***************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def data_prep_daily(ticker=None, currency_code=None):
    print("{} : === {} DATA PREPERATION STARTED ===".format(dateNow(), currency_code))
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    populate_bot_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, daily=True)
    print("{} : === DATA PREPERATION COMPLETED ===".format(dateNow()))
    if type(ticker) != type(None):
        report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(dateNow(), ticker))
    elif type(currency_code) != type(None):
        report_to_slack("{} : === {} DATA PREPERATION COMPLETED ===".format(dateNow(), currency_code))
    else:
        report_to_slack("{} : === DATA PREPERATION DAILY COMPLETED ===".format(dateNow()))


def data_prep_check_new_ticker(ticker=None, currency_code=None):
    print("{} : === DATA PREPERATION CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    date_identifier = "trading_day"
    new_ticker = get_new_tickers_from_bot_data(start_date, start_date2, date_identifier, ticker=ticker, currency_code=currency_code)["ticker"].to_list()
    if (len(new_ticker) > 0):
        print(f"Found {len(new_ticker)} New Ticker {tuple(new_ticker)}")
        populate_bot_data(start_date=start_date2, end_date=end_date, ticker=new_ticker, new_ticker=True)
        print("{} : === DATA PREPERATION CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === DATA PREPERATION CHECK NEW TICKER COMPLETED ===".format(dateNow()))


def data_prep_history():
    print("{} : === DATA PREPERATION HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print("Data preparation history started!")
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    ticker = get_active_universe()["ticker"].tolist()

    populate_bot_data(start_date=start_date, end_date=end_date, ticker=ticker, history=True)
    print("{} : === DATA PREPERATION HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === DATA PREPERATION HISTORY COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** VOLATILITY INFER ***************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def infer_daily(ticker=None, currency_code=None):
    folder_check()
    print("{} : === {} VOLATILITY INFER STARTED ===".format(dateNow(), currency_code))
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    
    populate_vol_infer(start_date, end_date, ticker=ticker, currency_code=currency_code, daily=True)

    print("{} : === VOLATILITY INFER COMPLETED ===".format(dateNow()))
    if type(ticker) != type(None):
        report_to_slack("{} : === {} VOLATILITY INFER COMPLETED ===".format(dateNow(), ticker))
    elif type(currency_code) != type(None):
        report_to_slack("{} : === {} VOLATILITY INFER COMPLETED ===".format(dateNow(), currency_code))
    else:
        report_to_slack("{} : === VOLATILITY INFER DAILY COMPLETED ===".format(dateNow()))


def infer_history():
    folder_check()
    print("{} : === VOLATILITY INFER HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")

    ticker = get_active_universe()["ticker"].tolist()
    # main_df = get_executive_data_download(start_date, end_date, ticker=ticker)
    # print(main_df)

    populate_vol_infer(start_date, end_date, ticker=ticker, history=True)
    print("{} : === VOLATILITY INFER HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === VOLATILITY INFER HISTORY COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** OPTION MAKER CLASSIC ***********************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def option_maker_classic_check_new_ticker(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER CLASSIC CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    new_ticker = get_new_ticker_from_bot_backtest(ticker=ticker, currency_code=currency_code, uno=True, mod=mod)["ticker"].to_list()
    print(new_ticker)
    if (len(new_ticker) > 0):
        ticker_length = len(new_ticker)
        print(f"Found {ticker_length} New Ticker")
        if option_maker:
            populate_bot_classic_backtest(start_date=start_date, end_date=end_date, ticker=new_ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod)
            print("Option creation is finished")
        if null_filler:
            fill_bot_backtest_classic(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=new_ticker, currency_code=currency_code, mod=mod)
        print("{} : === OPTION MAKER CLASSIC CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === OPTION MAKER CLASSIC CHECK NEW TICKER COMPLETED ===".format(dateNow()))


def option_maker_daily_classic(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False):
    print("{} : === OPTION MAKER CLASSIC STARTED ===".format(dateNow()))
    latest_dates_db = get_backtest_latest_date(ticker=ticker, currency_code=currency_code, mod=mod, classic=True)
    start_date = latest_dates_db["max_date"].min()
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    if option_maker:
        populate_bot_classic_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_classic(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, mod=mod)
    print("{} : === OPTION MAKER CLASSIC COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER CLASSIC COMPLETED ===".format(dateNow()))


def option_maker_history_classic(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False):
    time_to_exp = check_time_to_exp(time_to_exp)
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    print("{} : === OPTION MAKER CLASSIC HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    if option_maker:
        populate_bot_classic_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_classic(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, mod=mod)
    print("{} : === OPTION MAKER CLASSIC HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER CLASSIC HISTORY COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** OPTION MAKER UNO ***************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def option_maker_uno_check_new_ticker(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UNO CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    new_ticker = get_new_ticker_from_bot_backtest(ticker=ticker, currency_code=currency_code, uno=True, mod=mod)["ticker"].to_list()
    print(new_ticker)
    if (len(new_ticker) > 0):
        ticker_length = len(new_ticker)
        print(f"Found {ticker_length} New Ticker")
        if option_maker:
            populate_bot_uno_backtest(start_date=start_date, end_date=end_date, ticker=new_ticker, time_to_exp=time_to_exp, mod=mod, infer=infer, new_ticker=True)
            print("Option creation is finished")
        if null_filler:
            fill_bot_backtest_uno(start_date=start_date, end_date=end_date, ticker=new_ticker, time_to_exp=time_to_exp, mod=mod, total_no_of_runs=total_no_of_runs, run_number=run_number)
        print("{} : === OPTION MAKER UNO CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === OPTION MAKER UNO CHECK NEW TICKER COMPLETED ===".format(dateNow()))


def option_maker_daily_uno(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UNO STARTED ===".format(dateNow()))
    latest_dates_db = get_backtest_latest_date(ticker=ticker, currency_code=currency_code, mod=mod, uno=True)
    start_date = latest_dates_db["max_date"].min()
    end_date = str_to_date(dateNow())
    latest_date = get_volatility_latest_date(ticker=ticker, currency_code=currency_code, infer=True)
    if start_date is None:
        start_date = latest_date
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    if option_maker:
        populate_bot_uno_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, infer=infer, daily=False)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_uno(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, total_no_of_runs=total_no_of_runs, run_number=run_number)
    print("{} : === OPTION MAKER UNO COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UNO COMPLETED ===".format(dateNow()))


def option_maker_history_uno(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UNO HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    if option_maker:
        populate_bot_uno_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, infer=infer, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_uno(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True, total_no_of_runs=total_no_of_runs, run_number=run_number)
    print("{} : === OPTION MAKER UNO HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UNO HISTORY COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** OPTION MAKER UCDC **************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def option_maker_ucdc_check_new_ticker(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UCDC CHECK NEW TICKER STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    start_date2 = str_to_date(droid_start_date_buffer())
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    new_ticker = get_new_ticker_from_bot_backtest(ticker=ticker, currency_code=currency_code, ucdc=True, mod=mod)["ticker"].to_list()
    print(new_ticker)
    if (len(new_ticker) > 0):
        ticker_length = len(new_ticker)
        print(f"Found {ticker_length} New Ticker")
        if option_maker:
            populate_bot_ucdc_backtest(start_date=start_date, end_date=end_date, ticker=new_ticker, time_to_exp=time_to_exp, mod=mod, infer=infer, new_ticker=True)
            print("Option creation is finished")
        if null_filler:
            fill_bot_backtest_ucdc(start_date=start_date, end_date=end_date, ticker=new_ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, total_no_of_runs=total_no_of_runs, run_number=run_number)
            print("{} : === OPTION MAKER UCDC CHECK NEW TICKER COMPLETED ===".format(dateNow()))
        report_to_slack("{} : === OPTION MAKER UCDC CHECK NEW TICKER COMPLETED ===".format(dateNow()))


def option_maker_daily_ucdc(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UCDC STARTED ===".format(dateNow()))
    latest_dates_db = get_backtest_latest_date(ticker=ticker, currency_code=currency_code, mod=mod, ucdc=True)
    start_date = latest_dates_db["max_date"].min()
    end_date = str_to_date(dateNow())
    latest_date = get_volatility_latest_date(ticker=ticker, currency_code=currency_code, infer=True)
    if start_date is None:
        start_date = latest_date
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    if option_maker:
        populate_bot_ucdc_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, infer=infer, daily=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_ucdc(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, total_no_of_runs=total_no_of_runs, run_number=run_number)
    print("{} : === OPTION MAKER UCDC COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UCDC COMPLETED ===".format(dateNow()))


def option_maker_history_ucdc(currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UCDC HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    if option_maker:
        populate_bot_ucdc_backtest(currency_code=currency_code, start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, mod=mod, infer=infer, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_ucdc(start_date=start_date, end_date=end_date, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True, total_no_of_runs=total_no_of_runs, run_number=run_number)
    print("{} : === OPTION MAKER UCDC HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === OPTION MAKER UCDC HISTORY COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** BOT STATISTIC ******************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def bot_statistic_classic(ticker=None, currency_code=None, time_to_exp=None):
    print("{} : === BOT STATISTIC CLASSIC STARTED ===".format(dateNow()))
    time_to_exp = check_time_to_exp(time_to_exp)
    populate_classic_statistic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)
    print("{} : === BOT STATISTIC CLASSIC COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === BOT STATISTIC CLASSIC COMPLETED ===".format(dateNow()))


def bot_statistic_ucdc(ticker=None, currency_code=None, time_to_exp=None):
    print("{} : === BOT STATISTIC UCDC STARTED ===".format(dateNow()))
    time_to_exp = check_time_to_exp(time_to_exp)
    populate_ucdc_statistic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)
    print("{} : === BOT STATISTIC UCDC COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === BOT STATISTIC UCDC COMPLETED ===".format(dateNow()))


def bot_statistic_uno(ticker=None, currency_code=None, time_to_exp=None):
    print("{} : === BOT STATISTIC UNO STARTED ===".format(dateNow()))
    time_to_exp = check_time_to_exp(time_to_exp)
    populate_uno_statistic(ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp)
    print("{} : === BOT STATISTIC UNO COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === BOT STATISTIC UNO COMPLETED ===".format(dateNow()))


# ************************************************************************************************************************************************************************************
# *************************** BOT STATISTIC ******************************************************************************************************************************************
# ************************************************************************************************************************************************************************************
def bot_ranking_history(ticker=None, currency_code=None, mod=False):
    folder_check()
    print("{} : === BOT RANKING HISTORY STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(droid_start_date())
    end_date = str_to_date(dateNow())
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, mod=mod, history=True)
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    print("{} : === BOT RANKING HISTORY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === BOT RANKING HISTORY COMPLETED ===".format(dateNow()))


def bot_ranking_daily(ticker=None, currency_code=None, mod=False):
    folder_check()
    print("{} : === BOT RANKING DAILY STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = get_bot_data_latest_date(daily=True)
    end_date = str_to_date(dateNow())
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, mod=mod)
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    print("{} : === BOT RANKING DAILY COMPLETED ===".format(dateNow()))
    report_to_slack("{} : === BOT RANKING DAILY COMPLETED ===".format(dateNow()))