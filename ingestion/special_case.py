
import sys
from ingestion.master_multiple import master_multiple_update
from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from general.slack import report_to_slack
from general.date_process import datetimeNow
from general.sql_process import do_function
from general.sql_output import fill_null_company_desc_with_ticker_name, fill_null_quandl_symbol, upsert_data_to_database
from general.sql_query import get_active_universe, get_active_universe_consolidated_by_field, get_data_from_table_name
from ingestion.data_from_dss import update_ticker_symbol_from_dss
from ingestion.data_from_rkd import (
    populate_intraday_latest_price_from_rkd)
from ingestion.data_from_dsws import (
    populate_universe_consolidated_by_isin_sedol_from_dsws, 
    update_company_desc_from_dsws,
    update_currency_code_from_dsws, 
    update_entity_type_from_dsws,
    update_ibes_currency_from_dsws, 
    update_industry_from_dsws,
    update_lot_size_from_dsws,
    update_mic_from_dsws, 
    update_ticker_name_from_dsws, 
    update_worldscope_identifier_from_dsws)
from general.table_name import (
    get_bot_backtest_table_name,
    get_bot_classic_backtest_table_name,
    get_bot_data_table_name,
    get_bot_latest_ranking_table_name,
    get_bot_ranking_table_name,
    get_bot_statistic_table_name,
    get_bot_ucdc_backtest_table_name,
    get_bot_uno_backtest_table_name,
    get_data_dividend_daily_rates_table_name,
    get_data_dividend_table_name,
    get_data_dss_table_name, 
    get_data_dsws_table_name,
    get_data_ibes_monthly_table_name,
    get_data_ibes_table_name, 
    get_data_vol_surface_inferred_table_name, 
    get_data_vol_surface_table_name,
    get_data_worldscope_summary_table_name, 
    get_fundamental_score_table_name,
    get_latest_bot_data_table_name,
    get_latest_bot_update_table_name,
    get_latest_vol_table_name,
    get_orders_position_table_name,
    get_orders_table_name, 
    get_quandl_table_name, 
    get_universe_client_table_name, 
    get_universe_consolidated_table_name,
    get_universe_rating_detail_history_table_name,
    get_universe_rating_history_table_name, 
    get_universe_rating_table_name
)

def ticker_changes(old_ticker, new_ticker):
    old_ticker_uid = old_ticker.replace(".", "").replace("-", "").replace("_", "")
    new_ticker_uid = new_ticker.replace(".", "").replace("-", "").replace("_", "")
    consolidated_universe = get_active_universe_consolidated_by_field(ticker=old_ticker)
    print(consolidated_universe)
    if(len(consolidated_universe) == 0):
        sys.exit("Please Make Sure Ticker in Consolidated Universe Table")
    consolidated_universe["origin_ticker"] = new_ticker
    consolidated_universe["consolidated_ticker"] = new_ticker
    print(consolidated_universe)
    upsert_data_to_database(consolidated_universe, get_universe_consolidated_table_name(), "uid", how="update", Text=True)
    populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=[new_ticker])
    consolidated_universe = get_active_universe_consolidated_by_field(ticker=new_ticker)
    print(consolidated_universe)
    do_function("universe_populate")


    universe = get_active_universe(ticker=[new_ticker], active=False)
    if(len(universe) == 0):
        sys.exit("Please Make Sure Ticker in Universe Table")
    ticker = universe["ticker"].to_list()
    update_ticker_name_from_dsws(ticker=ticker)
    update_ticker_symbol_from_dss(ticker=ticker)
    update_entity_type_from_dsws(ticker=ticker)
    update_lot_size_from_dsws(ticker=ticker)
    update_currency_code_from_dsws(ticker=ticker)
    update_ibes_currency_from_dsws(ticker=ticker)
    update_industry_from_dsws(ticker=ticker)
    update_company_desc_from_dsws(ticker=ticker)
    update_mic_from_dsws(ticker=ticker)
    update_worldscope_identifier_from_dsws(ticker=ticker)


    universe = get_active_universe(ticker=[new_ticker], active=False)
    fill_null_company_desc_with_ticker_name()
    fill_null_quandl_symbol()
    print(universe)
    table_name = get_universe_client_table_name()
    uid = "id"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Int=True)

    table_name = get_data_dss_table_name()
    uid = "dss_id"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_dsws_table_name()
    uid = "dsws_id"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_vol_surface_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)
    
    table_name = get_data_vol_surface_inferred_table_name()
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_quandl_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_fundamental_score_table_name()
    uid = "ticker"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_universe_rating_table_name()
    uid = "ticker"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_universe_rating_history_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_universe_rating_detail_history_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_dividend_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_dividend_daily_rates_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_ibes_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_ibes_monthly_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_data_worldscope_summary_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_backtest_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_uno_backtest_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_ucdc_backtest_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_classic_backtest_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_data_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_statistic_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_ranking_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_bot_latest_ranking_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_latest_vol_table_name()
    uid = "ticker"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_latest_bot_update_table_name()
    uid = "uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    data[uid] = data[uid].str.replace(old_ticker_uid, new_ticker_uid, regex=True)
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_latest_bot_data_table_name()
    uid = "ticker"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_orders_position_table_name()
    uid = "position_uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    table_name = get_orders_table_name()
    uid = "order_uid"
    data = get_data_from_table_name(table_name, ticker=[old_ticker], active=False)
    data["ticker"] = universe.loc[0, "ticker"]
    upsert_data_to_database(data, table_name, uid, how="update", Text=True)

    do_function("special_cases_1")
    do_function("master_ohlcvtr_update")
    master_ohlctr_update(history=True)
    master_tac_update()
    master_multiple_update()
    populate_intraday_latest_price_from_rkd(ticker=[new_ticker])
    # populate_latest_price(ticker=[new_ticker])
    
    report_to_slack("{} : === {} CHANGES TO {} ===".format(datetimeNow(), old_ticker, new_ticker))