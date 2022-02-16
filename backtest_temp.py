# from main_executive import data_prep_history

from general.date_process import backdate_by_month, dateNow, droid_start_date_buffer, str_to_date
from general.sql_query import get_active_universe, get_ticker_etf
from bot.main_file import populate_bot_data


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
    report = "DATA PREPERATION HISTORY COMPLETED"

if __name__ == "__main__":
    data_prep_history(currency_code=["USD", "HKD", "CNY"])