from ingestion.special_case import ticker_changes
from bot.data_download import get_new_ticker_from_classic_bot_backtest, get_new_ticker_from_uno_ucdc_bot_backtest
from bot.calculate_bot import get_ucdc, get_uno
from main_executive import daily_classic, daily_uno_ucdc
from general.date_process import dateNow, str_to_date
from general.sql_query import get_active_universe, get_active_universe_droid1, get_universe_by_region
from general.sql_output import fill_null_quandl_symbol
from bot.preprocess import dividend_daily_update, interest_daily_update
from pandas.core.series import Series
from general.sql_process import do_function
from ingestion.universe import (
    update_mic_from_dss,
    update_ticker_name_from_dsws,
    update_entity_type_from_dsws,
    update_lot_size_from_dss,
    update_currency_code_from_dss,
    populate_universe_consolidated_by_isin_sedol_from_dsws,
    update_industry_from_dsws,
    update_company_desc_from_dsws, update_ticker_symbol_from_dss,
    update_worldscope_identifier_from_dsws
)

from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from ingestion.master_multiple import master_multiple_update
from ingestion.master_data import (
    interest_update,
    dividend_updated,
    populate_intraday_latest_price, populate_latest_price,
    update_data_dss_from_dss,
    update_data_dsws_from_dsws,
    update_vix_from_dsws,
    update_quandl_orats_from_quandl,
    update_fundamentals_score_from_dsws,
    update_fundamentals_quality_value)
from ingestion.currency import (
    update_currency_price_from_dss,
    update_index_price_from_dss,
    update_utc_offset_from_timezone
)
from ingestion.ai_value import (
    populate_ibes_table,
    populate_macro_table,
    update_ibes_data_monthly_from_dsws,
    update_macro_data_monthly_from_dsws,
    update_worldscope_quarter_summary_from_dsws)
from general.sql_query import read_query

import django
import os
debug = os.environ.get("DJANGO_SETTINGS_MODULE",True)
if debug:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.production") # buat Prod DB
else:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.development") # buat test DB
django.setup()

def daily_na():
    ticker = get_universe_by_region(region_id="na")
    ticker = ticker["ticker"].to_list()
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()
    daily_classic(ticker=ticker)

def daily_ws():
    ticker = get_universe_by_region(region_id="ws")
    ticker = ticker["ticker"].to_list()
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()
    daily_classic(ticker=ticker)

def dlpa_weekly():
    print("Run DLPA")
    # main_portfolio.py --live --portfolio_period 0

    query = f"select distinct on (index, ticker, spot_date, forward_date) index, ticker, spot_date, forward_date "
    query += f"from client_portfolios where forward_tri is null and forward_return is null "
    query += f"and index_forward_price is not null and index_forward_return is not null "
    query += f"and (forward_date::date + interval '1 days')::date <= NOW()"
    client_portfolios_missing = read_query(query, table="client_portfolios")

    query = f"select distinct on (index, spot_date, forward_date) index, spot_date, forward_date "
    query += f"from client_portfolios where forward_tri is null and forward_return is null "
    query += f"and index_forward_price is null and index_forward_return is null "
    query += f"and (forward_date::date + interval '1 days')::date <= NOW()"
    client_portfolios_holiday = read_query(query, table="client_portfolios")

    # Select Data from dss_ohlcvtr and append.
    for index, row in client_portfolios_missing.head().iterrows():
        ticker = row["ticker"]
        spot_date = row["spot_date"]
        forward_date = row["forward_date"]
        print("{} : === This ticker {} is null on {} to {}===".format(
            dateNow(), ticker, spot_date, forward_date))

    # Holiday report
    for index, row in client_portfolios_holiday.head().iterrows():
        indices = row["index"]
        spot_date = row["spot_date"]
        forward_date = row["forward_date"]
        print("{} : === This index {} is Holiday from {} to {}===".format(
            dateNow(), indices, spot_date, forward_date))

        # report_to_slack("{} : === Start filled_holiday_client_portfolios ===".format(str(datetime.now())), args)
        do_function("filled_holiday_client_portfolios")

        # report_to_slack("{} : === Start migrate_client_portfolios ===".format(str(datetime.now())), args)
        do_function("migrate_client_portfolios")

        # report_to_slack("{} : === FINISH CLIENT PORTFOLIO ===".format(str(datetime.now())), args)
        do_function("latest_universe")

    # Post to Linkedin
    # Post to Facebook


def new_ticker_ingestion(ticker=None):
    update_ticker_name_from_dsws(ticker=ticker)
    update_ticker_symbol_from_dss(ticker=ticker)
    update_entity_type_from_dsws(ticker=ticker)
    update_lot_size_from_dss(ticker=ticker)
    update_currency_code_from_dss(ticker=ticker)
    update_industry_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_mic_from_dss(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)
    update_quandl_orats_from_quandl(ticker=ticker)
    populate_latest_price(ticker=ticker)
    if isinstance(ticker, Series) or isinstance(ticker, list):
        for tick in ticker:
            update_data_dss_from_dss(ticker=tick, history=True)
            update_data_dsws_from_dsws(ticker=tick, history=True)
            dividend_updated(ticker=tick)
    else:
        update_data_dss_from_dss(ticker=ticker, history=True)
        update_data_dsws_from_dsws(ticker=ticker, history=True)
        dividend_updated(ticker=ticker)
    # do_function("master_ohlcvtr_update")
    # master_ohlctr_update()
    # master_tac_update()
    # master_multiple_update()


def daily_process_ohlcvtr(region_id = None):
    if(type(region_id) != type(None)):
        ticker = get_universe_by_region(region_id=region_id)
    else:
        ticker = get_active_universe()
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)
    do_function("special_cases_1")
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()

def daily_ingestion(region_id=None):
    dlp_ticker = get_active_universe_droid1()
    print(dlp_ticker)
    if(region_id == None):
        droid2_ticker = get_active_universe()
    else:
        droid2_ticker = get_universe_by_region(region_id=region_id)
    print(droid2_ticker)
    dlp_ticker = dlp_ticker.loc[dlp_ticker["ticker"].isin(droid2_ticker["ticker"].to_list())]
    print(dlp_ticker)
    ticker = droid2_ticker.loc[~droid2_ticker["ticker"].isin(dlp_ticker["ticker"].to_list())]
    ticker = ticker["ticker"].to_list()
    print(ticker)
    print(len(ticker))
    update_data_dss_from_dss(ticker=ticker)
    update_data_dsws_from_dsws(ticker=ticker)

# Main Process
if __name__ == "__main__":
    # update_currency_price_from_dss()
    # update_currency_code_from_dss(ticker=["MSFT.O", "AAPL.O"])
    update_lot_size_from_dss(ticker=["MSFT.O", "AAPL.O"], currency_code=None)
    # status = ticker_changes("SLA.L", "ABDN.L")
    # update_fundamentals_score_from_dsws(currency_code=["SGD"])
    # update_fundamentals_quality_value()
    # update_mic_from_dss()
    # update_ticker_symbol_from_dss(currency_code=["HKD"])
    from migrate import weekly_migrations, daily_migrations
    # update_ibes_data_monthly_from_dsws(ticker=[".HSI"])
    # weekly_migrations()
    # daily_ingestion()
    # update_ticker_name_from_dsws(currency_code=["USD", "EUR", "GBP"])
    # daily_migrations()  # triger ingestion function
    # daily_ingestion(region_id="na") # one region
    # ticker = get_universe_by_region(region_id="na")
    # populate_latest_price(ticker=ticker["ticker"])

    # populate_macro_table()
    # populate_ibes_table()
    # do_function("master_ohlcvtr_update")
    # master_ohlctr_update()
    # master_tac_update()
    # update_currency_price_from_dss()
    # interest_update()
    # dividend_daily_update()
    # interest_daily_update()

    # update_ticker_symbol_from_dss(ticker=None)
    # do_function("universe_populate")
    # populate_intraday_latest_price(currency_code=["KRW"])
    # populate_intraday_latest_price(ticker=["ABDN.L"])
    # populate_latest_price(currency_code=["CNY"])
    # ticker = get_universe_by_region(region_id="na")
    # populate_latest_price(ticker=ticker["ticker"])
    # do_function("universe_populate")
    # daily_migrations()
    # populate_macro_table()
    # populate_ibes_table()
    # update_quandl_orats_from_quandl()


    # do_function("special_cases_1")
    # do_function("master_ohlcvtr_update")
    # master_ohlctr_update()
    # master_tac_update()
    # master_multiple_update()

    # interest_update()
    # dividend_daily_update()
    # interest_daily_update()
    # update_utc_offset_from_timezone()
    # update_index_price_from_dss(currency_code=["USD"])
    # print("Done")
    # report_to_slack("TEST")