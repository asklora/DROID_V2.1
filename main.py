import os
import sys
from general.sql_process import do_function
from ingestion.universe import (
    update_ticker_name_from_dsws, 
    update_entity_type_from_dsws, 
    update_lot_size_from_dss,
    update_currency_code_from_dss,
    populate_universe_consolidated_by_isin_sedol_from_dsws,
    update_industry_from_dsws, 
    update_company_desc_from_dsws,
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
def update_master_data(ticker=None, currency_code=None):
    update_quandl_orats_from_quandl()
    update_vix_from_dsws()
    do_function("universe_populate")
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
    do_function("universe_populate")
    update_ticker_name_from_dsws(ticker=ticker)
    update_entity_type_from_dsws(ticker=ticker)
    update_lot_size_from_dss(ticker=ticker)
    update_currency_code_from_dss(ticker=ticker)
    update_industry_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)

if __name__ == "__main__":
    print(DB_URL_WRITE)
    print(DB_URL_READ)
    #update_utc_offset_from_timezone()
    # ticker = ["AAPL.O"]
    # master_tac_update()
    # master_multiple_update()
    # ticker=["AAPL.O", "MSFT.O"]
    # currency_code=["USD"]
    print("Done")