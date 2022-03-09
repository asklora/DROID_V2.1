# from main_executive import data_prep_history
from datetime import datetime
from bot.data_download import get_bot_option_type
from general.sql_output import truncate_table, upsert_data_to_database
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
from general.sql_query import get_active_universe, get_ticker_etf, read_query
from bot.main_file import populate_bot_data
from bot.final_model import populate_vol_infer

def data_prep_history(currency_code=None):
    print("{} : === DATA PREPERATION HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(7))
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
    """ train bot model, details refer to populate_vol_infer() """

    folder_check()
    print("{} : === VOLATILITY TRAIN MODEL STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(backdate_by_month(7))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_vol_infer(start_date, end_date, ticker=ticker, currency_code=currency_code, train_model=True)

def infer_history(currency_code=None):
    folder_check()
    print("{} : === VOLATILITY INFER HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(7))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    ticker = get_active_universe(currency_code=currency_code)["ticker"].tolist()
    populate_vol_infer(start_date, end_date, ticker=ticker, history=True)

def option_maker_history_uno(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UNO HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(7))
    end_date = str_to_date(dateNow())
    if option_maker:
        populate_bot_uno_backtest(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, infer=infer, history=True)
        print("Option creation is finished")
    if null_filler:
        fill_bot_backtest_uno(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, time_to_exp=time_to_exp, mod=mod, history=True, total_no_of_runs=total_no_of_runs, run_number=run_number)

def option_maker_history_ucdc(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False, infer=True, total_no_of_runs=1, run_number=0):
    time_to_exp = check_time_to_exp(time_to_exp)
    print("{} : === OPTION MAKER UCDC HISTORY STARTED ===".format(dateNow()))
    start_date = str_to_date(backdate_by_month(7))
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
    start_date = str_to_date(backdate_by_month(7))
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
    start_date = str_to_date(backdate_by_month(7))
    end_date = str_to_date(dateNow())
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, time_to_exp=time_to_exp, bot_labeler_train = True, bots_list=bots_list)

def bot_ranking_history(ticker=None, currency_code=None, mod=False, time_to_exp=time_to_expiry):
    folder_check()
    print("{} : === BOT RANKING HISTORY STARTED ===".format(dateNow()))
    if(type(ticker) == type(None) and type(currency_code) == type(None)):
        ticker = get_active_universe()["ticker"].tolist()
    start_date = str_to_date(backdate_by_month(7))
    end_date = str_to_date(dateNow())
    print(f"The start date is set as: {start_date}")
    print(f"The end date is set as: {end_date}")
    populate_bot_labeler(start_date=start_date, end_date=end_date, ticker=ticker, time_to_exp=time_to_exp, currency_code=currency_code, mod=mod, history=True)

def get_best_backtest_bot(bot_id, time_exp, time_exp_str, bot_type, option_type):
    if(bot_type == "CLASSIC"):
        query = f"select backtest.uid, backtest.spot_date, '{bot_type}' as bot_type, '{option_type}' as bot_option_type, backtest.time_to_exp, backtest.drawdown_return, "
        query += f" 0 as delta_churn, backtest.avg_delta::double precision, 0 as v1, backtest.bot_return, backtest.expiry_return, backtest.ticker, "
        query += f"backtest.bot_id from bot_{bot_type.lower()}_backtest backtest where time_to_exp = {time_exp} and exists ( "
        query += f"select 1 from bot_ranking rank where rank_1_{time_exp_str}='{bot_type.lower()}_{option_type}_{time_exp_str}' and rank.ticker = backtest.ticker and rank.spot_date=backtest.spot_date)"
    else:
        query = f"select backtest.uid, backtest.spot_date, '{bot_type}' as bot_type, '{option_type}' as bot_option_type, backtest.time_to_exp, backtest.drawdown_return, "
        query += f"backtest.delta_churn, backtest.avg_delta::double precision, backtest.v1, backtest.bot_return, backtest.expiry_return, backtest.ticker, "
        query += f"backtest.bot_id from bot_{bot_type.lower()}_backtest backtest where time_to_exp = {time_exp} and option_type='{option_type}' and exists ( "
        query += f"select 1 from bot_ranking rank where rank_1_{time_exp_str}='{bot_type.lower()}_{option_type}_{time_exp_str}' and rank.ticker = backtest.ticker and rank.spot_date=backtest.spot_date)"
    data = read_query(query)
    return data

def update_best_bot():
    truncate_table("bot_backtest")
    bot = get_bot_option_type()
    bot = bot.loc[bot["bot_type"].isin(["CLASSIC", "UNO", "UCDC"])]
    for index, row in bot.iterrows():
        result = get_best_backtest_bot(row["bot_id"], row["time_to_exp"], row["time_to_exp_str"], row["bot_type"], row["bot_option_type"])
        upsert_data_to_database(result, "bot_backtest", "uid", how="update", Text=True)

import argparse
parser = argparse.ArgumentParser(description="Loratech")
parser.add_argument("-data_prep", "--data_prep", type=bool, help="data_prep", default=False)
parser.add_argument("-training", "--training", type=bool, help="training", default=False)
parser.add_argument("-infer", "--infer", type=bool, help="infer", default=False)
parser.add_argument("-uno", "--uno", type=bool, help="uno", default=False)
parser.add_argument("-ucdc", "--ucdc", type=bool, help="ucdc", default=False)
parser.add_argument("-classic", "--classic", type=bool, help="classic", default=False)
parser.add_argument("-currency_code", "--currency_code", nargs="+", help="currency_code", default=None)
parser.add_argument("-split", "--split", type=int, help="currency_code", default=0)
args = parser.parse_args()

if __name__ == "__main__":
    if(args.data_prep):
        data_prep_history(currency_code=["USD", "HKD", "CNY"])
        infer_history(currency_code=["USD", "HKD", "CNY"])
    if(args.training):
        train_model(currency_code=["USD", "HKD", "CNY"])
        train_lebeler_model(currency_code=["USD", "HKD", "CNY"])
    if(args.uno):
        if("USD" in  args.currency_code):
            ticker = get_active_universe(currency_code=args.currency_code)["ticker"].to_list()
            ticker_list = [ticker[0:300], ticker[300:]]
            option_maker_history_uno(ticker=ticker_list[args.split], option_maker=True, null_filler=True)
        else:
            option_maker_history_uno(currency_code=args.currency_code, option_maker=True, null_filler=True, infer=args.infer)
        if("USD" in  args.currency_code):
            bot_statistic_uno(currency_code=["USD", "HKD", "CNY"])
            bot_ranking_history(currency_code=["USD", "HKD", "CNY"])
            update_best_bot()
        print(datetime.now())
    if(args.ucdc):
        if("USD" in  args.currency_code):
            ticker = get_active_universe(currency_code=args.currency_code)["ticker"].to_list()
            ticker_list = [ticker[0:300], ticker[300:]]
            option_maker_history_ucdc(ticker=ticker_list[args.split], option_maker=True, null_filler=True)
        else:
            option_maker_history_ucdc(currency_code=args.currency_code, option_maker=True, null_filler=True, infer=args.infer)
        if("USD" in  args.currency_code):
            bot_statistic_ucdc(currency_code=["USD", "HKD", "CNY"])
        print(datetime.now())
    if(args.classic):
        option_maker_history_classic(currency_code=["USD", "HKD", "CNY"], option_maker=True, null_filler=True)
        bot_statistic_classic(currency_code=["USD", "HKD", "CNY"])
        print(datetime.now())
    pass
