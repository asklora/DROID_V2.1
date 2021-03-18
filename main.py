import os
import sys
from pandas.core.series import Series
from general.sql_process import do_function
from ingestion.universe import (
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
    update_data_dss_from_dss,
    update_data_dsws_from_dsws,
    update_vix_from_dsws, 
    update_quandl_orats_from_quandl,
    update_fundamentals_score_from_dsws,
    update_fundamentals_quality_value)
from ingestion.currency import (
    update_currency_price_from_dss, 
    update_utc_offset_from_timezone
    )
from global_vars import DB_URL_READ, DB_URL_WRITE


def new_ticker_ingestion(ticker=None):
    update_ticker_name_from_dsws(ticker=ticker)
    update_ticker_symbol_from_dss(ticker=ticker)
    update_entity_type_from_dsws(ticker=ticker)
    update_lot_size_from_dss(ticker=ticker)
    update_currency_code_from_dss(ticker=ticker)
    update_industry_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)
    update_quandl_orats_from_quandl(ticker=ticker)
    if isinstance(ticker,Series) or isinstance(ticker,list):
        for tick in ticker:
            update_data_dss_from_dss(ticker=tick)
            update_data_dsws_from_dsws(ticker=tick)
            dividend_updated(ticker=tick)
    else:
        update_data_dss_from_dss(ticker=ticker)
        update_data_dsws_from_dsws(ticker=ticker)
        dividend_updated(ticker=ticker)
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()
    

def update_master_data(ticker=None, currency_code=None):
    update_quandl_orats_from_quandl(ticker=[])
    update_vix_from_dsws()
    # do_function("universe_populate")
    update_data_dss_from_dss(ticker=ticker, currency_code=currency_code)
    update_data_dsws_from_dsws(ticker=ticker, currency_code=currency_code)
    do_function("master_ohlcvtr_update")
    master_ohlctr_update()
    master_tac_update()
    master_multiple_update()
    #do_function("universe_update_last_ingestion")
    dividend_updated(ticker=ticker, currency_code=currency_code)
    # dividend_daily_update()
    interest_update()
    # interest_daily_update(currency_code=currency_code)
    update_fundamentals_score_from_dsws(ticker=ticker, currency_code=currency_code)
    update_fundamentals_quality_value(ticker=ticker, currency_code=currency_code)

def update_currency_data():
    update_utc_offset_from_timezone()
    update_currency_price_from_dss()

def update_universe_data(ticker=None):
    populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=ticker)
    # do_function("universe_populate")
    update_ticker_name_from_dsws(ticker=ticker)
    update_entity_type_from_dsws(ticker=ticker)
    update_lot_size_from_dss(ticker=ticker)
    update_currency_code_from_dss(ticker=ticker)
    update_industry_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)

if __name__ == "__main__":
    # populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=ticker)
    # update_quandl_orats_from_quandl(ticker='MSFT.O')
    update_data_dsws_from_dsws(ticker="1COV.F", history=True)
    # update_ticker_symbol_from_dss()
    # do_function("master_ohlcvtr_update")
    # master_ohlctr_update()
    # master_tac_update()
    # master_multiple_update()
    print("Done")