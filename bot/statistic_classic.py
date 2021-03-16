import pandas as pd

from general.date_process import backdate_by_month, dateNow, str_to_date
from bot.data_download import get_bot_backtest_data
from general.table_name import get_bot_statistic_table_name
from general.sql_output import upsert_data_to_database

from global_vars import statistics_lookback_list

def populate_classic_statistic(ticker=None, currency_code=None, time_to_exp=None):

    def statistic_calculator(df):
        """For each ticker use the past monthly (horizon + 3 months) OR 1 years OR 3 years- eligible rows =
         all dates that have expired or events triggered
         num_rows = # rows in the ticker in past months/1 year/3 years with today > expiration or events triggered
         https://loratechai.atlassian.net/wiki/spaces/WEB/pages/56623116/Classic+SLTP+plan+template+Req
         """

        df2 = df.copy()
        df = df2.copy()
        bench_df = pd.DataFrame()

        # ******************************************************************************************
        # Adding columns(or dummy columns) to df for further calculations.

        df["bot_return"] = df["bot_return"].astype(float)
        df = df.assign(expiry_return=df["expiry_return"].astype(float))
        df = df.assign(bm=df.expiry_price.astype(float) / df.spot_price.astype(float) - 1)
        # dummy columns
        df = df.assign(tp_num=0)
        df = df.assign(sl_num=0)
        df = df.assign(return_pos=0)
        df = df.assign(return_neg=0)

        # ******************************************************************************************
        # These will be used for counting the occurrences.
        df.loc[df["event"] == "stop_loss", "sl_num"] = 1
        df.loc[df["event"] == "take_profit", "tp_num"] = 1
        df.loc[df["bot_return"] > 0, "return_pos"] = 1
        df.loc[df["bot_return"] < 0, "return_neg"] = 1

        # ******************************************************************************************

        #  avg return where returns >0 and where returns  < 0
        ret_pos_neg = df.groupby(["ticker","time_to_exp"])["bot_return"].agg([("return_neg", lambda x: x[x < 0].mean()), ("return_pos", lambda x: x[x > 0].mean())])
        ret_pos_neg.reset_index(inplace=True)
        df = df.assign(duration_days=round(df.duration * 365))

        d = {"mean":"avg_days_max_profit"}
        days_tp_sl = df.groupby(["ticker","time_to_exp", "event"]).agg({"duration_days": ["mean"]}).rename(columns=d)
        days_tp_sl.columns = days_tp_sl.columns.droplevel(0)
        days_tp_sl.reset_index(inplace=True)

        # avg_days = average (event_date - spot_date)
        days_events = df.groupby(["ticker","time_to_exp"]).agg({"duration_days": ["mean"]})
        days_events.reset_index(inplace=True)
        bench_df["avg_days"] = days_events["duration_days","mean"]

        returns = df.groupby(["ticker", "time_to_exp"]).agg({"bot_return": ["count","mean", "min"], "return_pos": ["sum"],
                                                              "return_neg": ["sum"], "sl_num": ["sum"], "tp_num": ["sum"],
                                                              "duration": ["mean"], "bm": ["mean","min"], "expiry_return": ["mean"], "drawdown_return": ["min"]})
        returns.reset_index(inplace=True)

        # Adding ticker and time_to_exp to output df(bench_df)
        bench_df["ticker"] = returns["ticker",""]
        bench_df["time_to_exp"] = returns["time_to_exp", ""]

        # pct_profit = (returns >0) / num_rows
        bench_df["pct_profit"] = returns["return_pos", "sum"] / returns["bot_return", "count"]

        # pct_losses = (returns < 0) / num_rows
        bench_df["pct_losses"] = returns["return_neg", "sum"] / returns["bot_return", "count"]

        # avg_profit = avg return where returns >0
        bench_df["avg_profit"] = ret_pos_neg["return_pos"]

        # avg_loss = avg return where returns  < 0
        bench_df["avg_loss"] = ret_pos_neg["return_neg"]

        # avg_return = avg return
        bench_df["avg_return"] = returns["bot_return", "mean"]

        # pct_tp (out of all cases) = num of TP rows / all rows
        bench_df["pct_max_profit"] = returns["tp_num", "sum"] / returns["bot_return", "count"]

        # pct_sl (out of all cases) = num of SL rows / all rows
        bench_df["pct_max_loss"] = returns["sl_num", "sum"] / returns["bot_return", "count"]

        # ann_avg_return = average  = avg_return / duration
        bench_df["ann_avg_return"] = returns["bot_return", "mean"] / returns["duration", "mean"]

        # ann_avg_return_bm = avg_bm_return = average expiry_return’s (X 12 for 1M and X4 for 3M)
        bench_df["ann_avg_return_bm"] = returns["expiry_return", "mean"]

        for time_exp in time_to_exp:
            days = int(round((time_exp * 365), 0)) #time_exp to days
            months = (12 / (days / 30)) # days to month_exp
            bench_df.loc[bench_df["time_to_exp"] == time_exp, "ann_avg_return_bm"] = bench_df["ann_avg_return_bm"] * int(round(months, 0))

        # avg_return_bm = avg_bm_return = average expiry_return’s
        bench_df["avg_return_bm"] = returns["expiry_return", "mean"]

        # avg_days_max_profit = average (event_date - spot_date) for event = ‘TP’
        bench_df = bench_df.merge(days_tp_sl[days_tp_sl.event == "TP"], on=["ticker","time_to_exp"], how="outer")

        # avg_days_sl = average (event_date - spot_date) for event = ‘SL’
        days_tp_sl.rename(columns={"avg_days_max_profit": "avg_days_max_loss"}, inplace=True)
        bench_df = bench_df.merge(days_tp_sl[days_tp_sl.event == "SL"], on=["ticker","time_to_exp"], how="outer")
        bench_df = bench_df.drop(["event_x","event_y"], 1)
        bench_df.fillna({"avg_days_max_profit": 0, "avg_days_max_loss": 0}, inplace=True)

        # max_loss_plan = min (return)
        bench_df["max_loss_bot"] = returns["bot_return", "min"]

        # max_loss_bm = min (bm_returns)
        bench_df["max_loss_bm"] = returns["drawdown_return","min"]

        bench_df = bench_df.infer_objects()

        return bench_df

    lookback_horizon = max(statistics_lookback_list)
    start_date = backdate_by_month(lookback_horizon)
    end_date = dateNow()
    df = get_bot_backtest_data(start_date=start_date, end_date=end_date, time_to_exp=time_to_exp, ticker=ticker, currency_code=currency_code, classic=True, not_null=True)
    print(df)

    option_type_list = ["classic"]
    bench_df_main = pd.DataFrame()

    for option_t in option_type_list:
        for lookback in statistics_lookback_list:
            df_t = df.copy()
            temp_date = str_to_date(backdate_by_month(lookback))
            temp_df = df_t.loc[df_t.spot_date >= temp_date, :]
            if len(temp_df) != 0:
                bench_df_1 = statistic_calculator(temp_df)
                bench_df_1["lookback"] = lookback
                bench_df_1["option_type"] = option_t
                bench_df_main = bench_df_main.append(bench_df_1)
    
    # new changes uid
    bench_df_main["bot_type"] = "CLASSIC"
    bench_df_main["uid"] = bench_df_main["ticker"] + "_" + bench_df_main["bot_type"] + "_" + bench_df_main["option_type"] + "_" + bench_df_main["lookback"].astype(str) + "_" + bench_df_main["time_to_exp"].astype(str)
    bench_df_main["uid"] = bench_df_main["uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True)
    bench_df_main["uid"] = bench_df_main["uid"].str.strip()

    table_name = get_bot_statistic_table_name()
    upsert_data_to_database(bench_df_main, table_name, "uid", how="update", cpu_count=True, Text=True)