import pandas as pd
from general.table_name import get_quandl_table_name
from general.slack import report_to_slack
from general.sql_process import do_function
from general.sql_output import upsert_data_to_database, update_ingestion_update_time
from general.sql_query import get_active_universe_by_quandl_symbol
from general.date_process import dateNow, datetimeNow, droid_start_date
from datasource.quandl import read_quandl_csv
from es_logging.logger import log2es

@update_ingestion_update_time(get_quandl_table_name())
@log2es("ingestion")
def update_quandl_orats_from_quandl(ticker=None, quandl_symbol=None):
    print("{} : === Quandl Start Ingestion ===".format(datetimeNow()))
    end_date = dateNow()
    start_date = droid_start_date()
    print("=== Getting data from Quandl ===")
    result = pd.DataFrame({"date":[],"stockpx":[],
        "iv30":[],"iv60":[],"iv90":[],
        "m1atmiv":[],"m1dtex":[],
        "m2atmiv":[],"m2dtex":[],
        "m3atmiv":[],"m3dtex":[],
        "m4atmiv":[],"m4dtex":[],
        "slope":[],"deriv":[],
        "slope_inf":[],"deriv_inf":[],
        "10dclsHV":[],"20dclsHV":[],"60dclsHV":[],"120dclsHV":[],"252dclsHV":[],
        "10dORHV":[],"20dORHV":[],"60dORHV":[],"120dORHV":[],"252dORHV":[]}, index=[])
    quandl_symbol_list = get_active_universe_by_quandl_symbol(ticker=ticker, quandl_symbol=quandl_symbol)
    print(quandl_symbol_list)
    for index, row in quandl_symbol_list.iterrows():
        data_from_quandl = read_quandl_csv(row["ticker"], row["quandl_symbol"], start_date, end_date)
        if (len(data_from_quandl) > 0):
            result = result.append(data_from_quandl)
    print("=== Getting data from Quandl DONE ===")
    if(len(result) > 0):
        result = result[["uid", "ticker", "trading_day","stockpx",
            "iv30","iv60","iv90",
            "m1atmiv", "m1dtex","m2atmiv","m2dtex",
            "m3atmiv","m3dtex","m4atmiv","m4dtex",
            "slope","deriv","slope_inf", "deriv_inf"]]
        print(result)
        upsert_data_to_database(result, get_quandl_table_name(), "uid", how="update", Text=True)
        do_function("data_vol_surface_update")
        do_function("latest_vol")
        report_to_slack("{} : === Quandl Updated ===".format(datetimeNow()))