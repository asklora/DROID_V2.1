import gc
import time
from general.slack import report_to_slack
import numpy as np
np.seterr(divide='ignore', invalid='ignore')
import pandas as pd
pd.options.mode.chained_assignment = None
from tqdm import tqdm
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta
from bot.preprocess import make_multiples
from general.sql_output import upsert_data_to_database
from general.date_process import backdate_by_day, dateNow, str_to_date
from bot.data_download import get_bot_backtest_data, get_calendar_data, get_master_tac_price, get_latest_price
from general.table_name import get_bot_classic_backtest_table_name, get_latest_price_table_name, get_universe_rating_table_name
from bot.data_process import check_start_end_date, check_time_to_exp
from global_vars import (classic_business_day, sl_multiplier_1m, tp_multiplier_1m, sl_multiplier_3m, 
    tp_multiplier_3m, max_vol, min_vol, default_vol)

def populate_bot_classic_backtest(start_date=None, end_date=None, ticker=None, currency_code=None, time_to_exp=None, mod=False, history=False):
    time_to_exp = check_time_to_exp(time_to_exp)
    start_date, end_date = check_start_end_date(start_date, end_date)
    # The main function which calculates the volatilities and stop loss and take profit and write them to AWS.
    # vol_surface_data = get_vol_surface_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code, infer=infer)
    # currency_data = get_currency_data(currency_code=currency_code)
    # interest_rate_data = get_interest_rate_data()
    # dividends_data = get_dividends_data()
    # ******************************* Calculating the vols *************************************
    holidays_df = get_calendar_data(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)
    holidays_df["non_working_day"] = pd.to_datetime(holidays_df["non_working_day"])
    tac_data = get_master_tac_price(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)
    tac_data = tac_data.loc[tac_data["day_status"] == "trading_day"]
    main_multiples = make_multiples(tac_data)

    # ********************************************************************************************
    # Calculating the volatilities based on returns
    log_returns = np.log(main_multiples)
    log_returns_sq = np.square(log_returns)
    returns = log_returns_sq.rolling(512, min_periods=1).mean()
    c2c_vol_0_502 = np.sqrt(returns * 256)
    c2c_vol_0_502[c2c_vol_0_502.isnull()] = default_vol
    c2c_vol_0_502[c2c_vol_0_502 < min_vol] = min_vol
    c2c_vol_0_502[c2c_vol_0_502 > max_vol] = max_vol

    c2c_vol_0_502["spot_date"] = c2c_vol_0_502.index
    main_pred = c2c_vol_0_502.melt(id_vars="spot_date", var_name="ticker", value_name="classic_vol")
    main_pred = main_pred.merge(tac_data[["ticker", "trading_day", "tri_adj_close"]], how="inner", 
        left_on=["spot_date", "ticker"], right_on=["trading_day", "ticker"])
    del main_pred["trading_day"]
    main_pred.rename(columns={"tri_adj_close": "spot_price"}, inplace=True)

    # ********************************************************************************************
    # Adding vol periods to main dataframe.
    main_pred_temp = pd.DataFrame(columns=main_pred.columns)
    print("Insert Vol Period")
    for time_exp in time_to_exp:
        main_pred2 = main_pred.copy()
        vol_period = int(round((time_exp * classic_business_day), 0))
        if  vol_period <= 21:
            sl_multiplier = sl_multiplier_1m
            tp_multiplier = tp_multiplier_1m
        else:
            sl_multiplier = sl_multiplier_3m
            tp_multiplier = tp_multiplier_3m
        main_pred2["vol_period"] = vol_period
        main_pred2["time_to_exp"] = time_exp
        # Calculating stop loss and take profit levels.
        main_pred2["stop_loss"] = (sl_multiplier * main_pred2["classic_vol"] * time_exp ** 0.5 + 1) * main_pred2["spot_price"]
        main_pred2["take_profit"] = (tp_multiplier * main_pred2["classic_vol"] * time_exp ** 0.5 + 1) * main_pred2["spot_price"]

        print("Calculating Expiry Date")
        days = int(round((time_exp * 365), 0))
        main_pred2["expiry_date"] = main_pred2["spot_date"] + relativedelta(days=(days-1))
        # Code when we use business days
        # days = int(round((time_exp * 256), 0))
        # main_pred2["expiry_date"] = options_df2["spot_date"] + BDay(days-1)

        main_pred_temp = main_pred_temp.append(main_pred2)
        main_pred_temp.reset_index(drop=True, inplace=True)
        del main_pred2
    del main_pred
    main_pred = main_pred_temp.copy()
    del main_pred_temp
    print(main_pred)

    main_pred = main_pred[main_pred["classic_vol"].notna()]

    # making sure that expiry date is not holiday or weekend
    main_pred["expiry_date"] = pd.to_datetime(main_pred["expiry_date"])
    while(True):
        cond = main_pred["expiry_date"].apply(lambda x: x.weekday()) > 4
        main_pred.loc[cond, "expiry_date"] = main_pred.loc[cond, "expiry_date"] - BDay(1)
        if(cond.all() == False):
            break

    while(True):
        cond = main_pred["expiry_date"].isin(holidays_df["non_working_day"].to_list())
        main_pred.loc[cond, "expiry_date"] = main_pred.loc[cond, "expiry_date"] - BDay(1)
        if(cond.all() == False):
            break

    # ********************************************************************************************

    # The following columns will be filled later.
    main_pred["event_date"] = None
    main_pred["event_price"] = None
    main_pred["expiry_price"] = None
    main_pred["drawdown_return"] = None
    main_pred["event"] = None
    main_pred["bot_return"] = None
    main_pred["duration"] = None
    main_pred["pnl"] = None
    main_pred["bot_id"] = "CLASSIC_classic_" + main_pred["time_to_exp"].astype(str).str.replace(".", "", regex=True)
    main_pred["initial_delta"] = 100
    main_pred["current_delta"] = 100
    main_pred["avg_delta"] = 100
    main_pred["total_bot_share_num"] = 1

    # Adding UID
    main_pred["uid"] = main_pred["ticker"] + "_" + main_pred["spot_date"].astype(str) + "_" + \
        main_pred["time_to_exp"].astype(str)
    main_pred["uid"] = main_pred["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True)
    main_pred["uid"] = main_pred["uid"].str.strip()

    # Filtering the results for faster writing to AWS.
    main_pred2 = main_pred[main_pred.spot_date >= start_date].copy()

    # ********************************************************************************************
    # Updating latest_price_updates classic_vol column.

    vol_df = main_pred2[["ticker", "classic_vol", "spot_date"]]
    vol_df = vol_df.sort_values(by="spot_date", ascending=False)
    vol_df.reset_index(inplace=True, drop=True)
    
    spot_date = end_date
    
    aa = vol_df.groupby(["ticker"]).agg({"classic_vol": ["first"]})
    aa.reset_index(inplace=True)
    aa.columns = aa.columns.droplevel(1)
    if len(aa) > 0:
        aa["spot_date"] = spot_date
        latest_price = get_latest_price().drop(columns=["classic_vol"])
        aa = aa[["ticker", "classic_vol"]]
        aa = aa.merge(latest_price, on=["ticker"], how="left")
        print(aa)
        if not history:
            upsert_data_to_database(aa[["ticker", "classic_vol"]], get_universe_rating_table_name(), "ticker", how="update", cpu_count=True, Text=True)
            upsert_data_to_database(aa, get_latest_price_table_name(), "ticker", how="update", cpu_count=True, Text=True)
        # ********************************************************************************************
        table_name = get_bot_classic_backtest_table_name()
        if(mod):
            table_name += "_mod"
        upsert_data_to_database(main_pred2, table_name, "uid", how="ignore", cpu_count=True, Text=True)

def FillMissingDay(data, start, end):
    result = data[["ticker", "trading_day"]]
    result = result.sort_values(by=["trading_day"], ascending=True)
    daily = pd.date_range(start, end, freq="D")
    indexes = pd.MultiIndex.from_product([result["ticker"].unique(), daily], names=["ticker", "trading_day"])
    result = result.set_index(["ticker", "trading_day"]).reindex(indexes).reset_index().ffill(limit=1)
    result = result[result["trading_day"].apply(lambda x: x.weekday() not in [5, 6])]
    result["trading_day"] = result["trading_day"].astype(str)
    result["uid"]=result["trading_day"] + data["ticker"]
    result["uid"]=result["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
    result["uid"]=result["uid"].str.strip()
    result["trading_day"] = pd.to_datetime(result["trading_day"])
    data["trading_day"] = pd.to_datetime(data["trading_day"])
    result = result.merge(data, how="left", on=["uid", "ticker", "trading_day"])
    del data, daily,indexes
    gc.collect()
    return result

def ForwardBackwardFillNull(data, columns_field):
    data = data.sort_values(by="trading_day", ascending=False)
    data = data.infer_objects()
    result = data[["uid", "ticker", "trading_day", "volume", "currency_code", "day_status"]]
    universe = data["ticker"].drop_duplicates()
    universe =universe.tolist()
    for column in columns_field:
        price = data.pivot_table(index="trading_day", columns="ticker", values=column, aggfunc="first", dropna=False)
        price = price.reindex(columns=universe)
        price = price.ffill()
        price = pd.DataFrame(price.values, index=price.index, columns=price.columns)
        price["trading_day"] = price.index
        price = price.melt(id_vars="trading_day", var_name="ticker", value_name=column)
        price["trading_day"] = price["trading_day"].astype(str)
        price["uid"]=price["trading_day"] + data["ticker"]
        price["uid"]=price["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.replace(" ", "", regex=True)
        price["uid"]=price["uid"].str.strip()
        price["trading_day"] = pd.to_datetime(price["trading_day"])
        price = price.drop(columns=["trading_day", "ticker"])
        result = result.merge(price, on=["uid"], how="left")
        del price
        time.sleep(3)
    del data, universe
    gc.collect()
    return result

# *********************** Filling up the Null values **************************
def fill_bot_backtest_classic(start_date=None, end_date=None, time_to_exp=None, ticker=None, currency_code=None, mod=False):
    time_to_exp = check_time_to_exp(time_to_exp)
    tac_data = get_master_tac_price(start_date=start_date, end_date=end_date, ticker=ticker, currency_code=currency_code)
    tac_data = tac_data.sort_values(by=["currency_code", "ticker", "trading_day"], ascending=True)
    tac_data = FillMissingDay(tac_data, start_date, backdate_by_day(1))
    tac_data = ForwardBackwardFillNull(tac_data, ["total_return_index", "tri_adj_open", "tri_adj_high", "tri_adj_low", "tri_adj_close", "rsi", "fast_k", "fast_d"])
    null_df = get_bot_backtest_data(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, classic=True, mod=mod, null_filler=True)
    # ********************************************************************************************
    prices_df = tac_data.pivot_table(index=tac_data.trading_day, columns="ticker", values="tri_adj_close", aggfunc="first", dropna=False)
    prices_df = prices_df.ffill()
    prices_df = prices_df.bfill()
    def SLTP_fn(row):
        try:
            # Calculate the desired quantities row by row.
            prices_temp = prices_df.loc[(prices_df.index >= row.spot_date) & (prices_df.index <= row.expiry_date), row.ticker]
            if len(prices_temp) == 0:
                return row
            # Finding the index that take profit is triggered.
            tp_indices = np.argmax((prices_temp >= row.take_profit).values)
            if tp_indices == 0:
                tp_indices = -1
            # Finding the index that stop loss is triggered.
            sl_indices = np.argmax((prices_temp <= row.stop_loss).values)
            if sl_indices == 0:
                sl_indices = -1
            days = int(round((row.time_to_exp * 365), 0))
            temp_date = row.spot_date + relativedelta(days=days)
            if temp_date.weekday() > 4:
                temp_date = temp_date - BDay(1)

            if (sl_indices == -1) & (tp_indices == -1):
                # If none of the events are triggered.
                if (prices_temp.index[-1] < temp_date) & (temp_date > str_to_date(dateNow()) - BDay(1)):
                    # If the expiry date hasn"t arrived yet.
                    row.event = None
                    row["bot_return"] = None
                    row.event_date = None
                    row.event_price = None
                else:
                    # If none of the events are triggered and expiry date has arrived.
                    row.event = "NT"
                    row["bot_return"] = prices_temp[-1] / prices_temp[0] - 1
                    row.event_date = prices_temp.index[-1]
                    row.event_price = prices_temp[-1]
                    row.expiry_price = prices_temp[-1]
                    row.expiry_return = prices_temp[-1] / prices_temp[0] - 1
                    row.pnl = prices_temp[-1] - prices_temp[0]
                    row.duration = (pd.to_datetime(row.event_date) - pd.to_datetime(row.spot_date)).days
                    row.current_delta = 100
            else:
                # If one of the events is triggered.
                if sl_indices > tp_indices:
                    # If stop loss is triggered.
                    row.event = "SL"
                    row["bot_return"] = prices_temp[sl_indices] / prices_temp[0] - 1
                    row.event_date = prices_temp.index[sl_indices]
                    row.event_price = prices_temp[sl_indices]
                    row.expiry_price = prices_temp[-1]
                    row.expiry_return = prices_temp[-1] / prices_temp[0] - 1
                    row.duration = (pd.to_datetime(row.event_date) - pd.to_datetime(row.spot_date)).days
                    row.pnl = prices_temp[sl_indices] - prices_temp[0]
                    row.current_delta = 100

                else:
                    # If take profit is triggered.
                    row.event = "TP"
                    row["bot_return"] = prices_temp[tp_indices] / prices_temp[0] - 1
                    row.event_date = prices_temp.index[tp_indices]
                    row.event_price = prices_temp[tp_indices]
                    row.expiry_price = prices_temp[-1]
                    row.expiry_return = prices_temp[-1] / prices_temp[0] - 1
                    row.duration = (pd.to_datetime(row.event_date) - pd.to_datetime(row.spot_date)).days
                    row.pnl = prices_temp[tp_indices] - prices_temp[0]
                    row.current_delta = 100


            if prices_temp.index[-1] < temp_date:
                # If the expiry date hasn"t arrived yet.
                row["drawdown_return"] = None
            else:
                # If the expiry date is arrived.
                row["drawdown_return"] = min(prices_temp) / prices_temp[0] - 1
        except Exception as e:
            print("{} : === FILL OPTION CLASSIC ERROR === : {}".format(dateNow(), e))
        return row

    tqdm.pandas()
    print(null_df)
    if len(null_df) > 0:
        null_df = null_df.progress_apply(SLTP_fn, axis=1)
        print(null_df)
        # ********************************************************************************************
        print(f"Filling up the nulls is finished.")
        table_name = get_bot_classic_backtest_table_name()
        if(mod):
            table_name += "_mod"
        upsert_data_to_database(null_df, table_name, "uid", how="update", cpu_count=True, Text=True)


