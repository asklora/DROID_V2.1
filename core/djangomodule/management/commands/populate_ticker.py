from ingestion.firestore_migration import firebase_universe_update
import pandas as pd
import numpy as np
from django.utils import timezone
from django.core.management.base import BaseCommand
from core.djangomodule.general import generate_id
from ingestion.master_multiple import master_multiple_update
from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from ingestion.data_from_quandl import update_quandl_orats_from_quandl
from general.table_name import get_universe_client_table_name
from general.sql_output import (
    fill_null_quandl_symbol, 
    insert_data_to_database, 
    update_consolidated_activation_by_ticker, 
    update_ingestion_update_time)
from general.data_process import tuple_data
from general.date_process import dateNow
from general.sql_process import do_function
from general.sql_query import (
    get_active_universe,
    get_active_universe_by_created, 
    get_consolidated_universe_data, 
    get_universe_client)
from ingestion.data_from_dss import (
    populate_ticker_from_dss, 
    update_data_dss_from_dss, 
    update_ticker_symbol_from_dss)
from ingestion.data_from_dsws import (
    dividend_updated_from_dsws, 
    populate_universe_consolidated_by_isin_sedol_from_dsws, 
    update_company_desc_from_dsws,
    update_currency_code_from_dsws, 
    update_data_dsws_from_dsws, 
    update_entity_type_from_dsws, 
    update_fundamentals_quality_value, 
    update_fundamentals_score_from_dsws,
    update_ibes_currency_from_dsws, 
    update_ibes_data_monthly_from_dsws, 
    update_industry_from_dsws,
    update_lot_size_from_dsws,
    update_mic_from_dsws, 
    update_rec_buy_sell_from_dsws, 
    update_ticker_name_from_dsws, 
    update_worldscope_identifier_from_dsws, 
    update_worldscope_quarter_summary_from_dsws)

def firebase_update():
    firebase_universe_update(currency_code=["HKD"])
    firebase_universe_update(currency_code=["USD"])

def new_ticker_ingestion(ticker):
    try:
        status = f"Ticker Name Update"
        update_ticker_name_from_dsws(ticker=ticker)
        status = f"Ticker Symbols Update"
        update_ticker_symbol_from_dss(ticker=ticker)
        status = f"Entity Type Update"
        update_entity_type_from_dsws(ticker=ticker)
        status = f"Lot Size Update"
        update_lot_size_from_dsws(ticker=ticker)
        status = f"Currency Code Update"
        update_currency_code_from_dsws(ticker=ticker)
        status = f"Ticker Name Update"
        update_ibes_currency_from_dsws(ticker=ticker)
        status = f"Industry Update"
        update_industry_from_dsws(ticker=ticker)
        status = f"Comapny Description Update"
        update_company_desc_from_dsws(ticker=ticker)
        status = f"Ticker Name Update"
        update_mic_from_dsws(ticker=ticker)
        status = f"Worldscope Identifier Update"
        update_worldscope_identifier_from_dsws(ticker=ticker)
        status = f"Quandl Symbol Update"
        fill_null_quandl_symbol()
        status = f"Quandl Orats Update"
        update_quandl_orats_from_quandl(ticker=ticker)
        status = f"DSS Data Update"
        update_data_dss_from_dss(ticker=ticker, history=True)
        status = f"DSWS Data Update"
        update_data_dsws_from_dsws(ticker=ticker, history=True)
        status = f"Dividend Update"
        dividend_updated_from_dsws(ticker=ticker)
        status = f"OHLCVTR Update"
        do_function("special_cases_1")
        do_function("master_ohlcvtr_update")
        master_ohlctr_update(history=True)
        status = f"TAC Update"
        master_tac_update()
        status = f"Multiple Update"
        master_multiple_update()
        status = f"IBES Data Update"
        update_ibes_data_monthly_from_dsws(ticker=ticker, history=True)
        status = f"Worldscope Quarter Update"
        update_worldscope_quarter_summary_from_dsws(ticker=ticker, history=True)
        status = f"Rec Buy Sell Update"
        update_rec_buy_sell_from_dsws(ticker=ticker)
        status = f"Fundamentals Score Update"
        update_fundamentals_score_from_dsws(ticker=ticker)
        status = f"Fundamentals Quality Update"
        update_fundamentals_quality_value()
    except Exception as e:
        print("{} : === {} New Ticker Ingestion ERROR === : {}".format(dateNow(), status, e))

def delete_old_ticker(ticker):
    try:
        status = f"OHLCVTR Update"
        do_function("special_cases_1")
        do_function("master_ohlcvtr_update")
        master_ohlctr_update(history=True)
        status = f"TAC Update"
        master_tac_update()
        status = f"Multiple Update"
        master_multiple_update()

        query = f"delete from data_vol_surface where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_vol_surface_inferred where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_quandl where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_fundamental_score where ticker in {tuple_data(ticker)}; "
        query += f"delete from universe_rating where ticker in {tuple_data(ticker)}; "
        query += f"delete from master_ohlcvtr where ticker in {tuple_data(ticker)}; "
        query += f"delete from master_tac where ticker in {tuple_data(ticker)}; "
        query += f"delete from master_multiple where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_dividend where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_dividend_daily_rates where ticker in {tuple_data(ticker)}; "
        query += f"delete from universe_client where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_ibes where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_ibes_monthly where ticker in {tuple_data(ticker)}; "
        query += f"delete from data_worldscope_summary where ticker in {tuple_data(ticker)}; "
        query += f"delete from latest_bot_data where ticker in {tuple_data(ticker)}; "
        query += f"delete from latest_bot_ranking where ticker in {tuple_data(ticker)}; "
        query += f"delete from latest_bot_update where ticker in {tuple_data(ticker)}; "
        query += f"delete from latest_price where ticker in {tuple_data(ticker)}; "
        query += f"delete from latest_vol where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_ranking where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_statistic where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_data where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_backtest where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_uno_backtest where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_classic_backtest where ticker in {tuple_data(ticker)}; "
        query += f"delete from bot_ucdc_backtest where ticker in {tuple_data(ticker)}; "
        query += f"delete from universe_rating_detail_history where ticker in {tuple_data(ticker)}; "
        query += f"delete from universe_rating_history where ticker in {tuple_data(ticker)}; "
        query += f"delete from dlpa_model_stock where ticker in {tuple_data(ticker)}; "
        query += f"delete from universe_consolidated where universe_consolidated.origin_ticker in {tuple_data(ticker)}; "
        query += f"delete from universe_consolidated where universe_consolidated.consolidated_ticker in {tuple_data(ticker)}; "
        query += f"delete from universe where ticker in {tuple_data(ticker)}; "
    except Exception as e:
        print("{} : === {} Old Ticker Deletion ERROR === : {}".format(dateNow(), status, e))

def populate_ticker_monthly(client=None):
    update_ingestion_update_time('universe', finish=False)
    if(client is None):
        client = f"dZzmhmoA" #Client is ASKLORA
    universe_consolidated = get_consolidated_universe_data()
    new_universe1 = populate_ticker_from_dss(index_list=["0#.HSLMI"], manual=1)
    new_universe2 =  populate_ticker_from_dss(index_list=["0#.SPX", "0#.NDX"], isin=1)
    new_universe3 = populate_ticker_from_dss(index_list=["0#.SXXE"], isin=1)
    new_universe = new_universe1.append(new_universe2)
    new_universe = new_universe.append(new_universe3)

    # Add new ticker added to index to "universe"
    new_universe.to_csv("/home/loratech/all_universe.csv")
    new_universe = new_universe.loc[~new_universe["origin_ticker"].isin(universe_consolidated["origin_ticker"].to_list())]
    new_universe.to_csv("/home/loratech/new_universe.csv")

    do_function("universe_populate")
    old_ticker = get_active_universe(currency_code=["HKD", "USD", "EUR"])
    consolidated = get_consolidated_universe_data()
    consolidated = consolidated.loc[consolidated["consolidated_ticker"].isin(old_ticker["ticker"].to_list())]
    new_ticker = pd.read_csv("/home/loratech/all_universe.csv")
    delete_ticker = consolidated.loc[~consolidated["origin_ticker"].isin(new_ticker["origin_ticker"].to_list())]
    active_ticker = consolidated.loc[consolidated["origin_ticker"].isin(new_ticker["origin_ticker"].to_list())]
    update_consolidated_activation_by_ticker(ticker=delete_ticker["origin_ticker"].to_list(), is_active=False)
    update_consolidated_activation_by_ticker(ticker=active_ticker["origin_ticker"].to_list(), is_active=True)
    do_function("universe_populate")

    new_universe = pd.read_csv("/home/loratech/new_universe.csv")
    new_universe = new_universe[["origin_ticker", "source_id", "use_isin", "use_manual"]]
    new_universe["use_manual"] = np.where(new_universe["use_manual"] == 1, True, False)
    new_universe["use_isin"] = np.where(new_universe["use_isin"] == 1, True, False)
    new_universe["uid"] = new_universe["origin_ticker"]
    for index, row in new_universe.iterrows():
        new_universe.loc[index, "uid"] = generate_id(8)
    new_universe["created"] = timezone.now().date()
    new_universe["updated"] = timezone.now().date()
    new_universe["has_data"] = False
    new_universe["source_id"] = f"DSS"
    new_universe["is_active"] = True
    new_universe["isin"] = None
    new_universe["cusip"] = None
    new_universe["use_cusip"] = False
    new_universe["sedol"] = None
    new_universe["use_sedol"] = False
    new_universe["permid"] = None
    new_universe["consolidated_ticker"] = new_universe["origin_ticker"]
    print(new_universe)
    populate_universe_consolidated_by_isin_sedol_from_dsws(manual_universe=new_universe.loc[new_universe["use_manual"] == True], universe=new_universe.loc[new_universe["use_isin"] == True])
    print(new_universe)
    do_function("universe_populate")
    ticker = get_active_universe_by_created(created=dateNow())
    universe_client = get_universe_client(client_uid=[client])
    new_universe_client = ticker.loc[~ticker["ticker"].isin(universe_client["ticker"].to_list())][["ticker"]]
    new_universe_client["client_uid"] = client
    new_universe_client["created"] = timezone.now()
    new_universe_client["updated"] = timezone.now()
    print(new_universe_client)
    insert_data_to_database(new_universe_client, get_universe_client_table_name(), how="append")
    new_ticker_ingestion(ticker["ticker"].to_list())
    update_ingestion_update_time('universe', finish=True)

def check_universe():
    old_universe = get_active_universe()
    do_function("universe_populate")
    new_universe = get_active_universe()
    new_ticker = new_universe.loc[~new_universe["origin_ticker"].isin(old_universe["ticker"].to_list())]
    deleted_ticker = old_universe.loc[~old_universe["ticker"].isin(new_universe["ticker"].to_list())]
    if(len(new_ticker)):
        new_ticker_ingestion(new_ticker["ticker"].to_list())
    if(len(deleted_ticker)):
        delete_old_ticker(deleted_ticker["ticker"].to_list())
    firebase_update()

class Command(BaseCommand):
    def add_arguments(self, parser):
        populate_ticker_monthly(client=None)
        firebase_update()