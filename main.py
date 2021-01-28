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
from ingestion.currency import update_currency_price_from_dss

if __name__ == "__main__":
    # do_function("universe_currency_update")
    # do_function("universe_last_ingestion_update")
    # do_function("universe_update")
    populate_universe_consolidated_by_isin_sedol_from_dsws()
    #update_worldscope_identifier_from_dsws()
    print("Done")