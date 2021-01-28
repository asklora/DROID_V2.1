import os
from general.sql_process import do_function
from ingestion.universe import (
    update_ticker_name_from_dsws, 
    update_entity_type_from_dsws, 
    update_lot_size_from_dss,
    update_currency_code_from_dss,
    populate_universe_consolidated_by_isin_sedol_from_dsws,
    update_country_from_dsws, 
    update_company_desc_from_dsws, 
    update_vix_from_dsws,
    update_worldscope_identifier_from_dsws
    )
from ingestion.currency import update_currency_price_from_dss, update_utc_offset_from_timezone

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
    update_country_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)

if __name__ == "__main__":
    #update_vix_from_dsws(ticker=ticker)
    print("Done")