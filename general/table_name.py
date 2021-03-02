def get_calendar_table_name():
    return "currency_calendar"

def get_universe_consolidated_table_name():
    return "universe_consolidated"

def get_universe_table_name():
    return "universe"

def get_universe_rating_table_name():
    return "universe_rating"

def get_country_table_name():
    return "country"

def get_currency_table_name():
    return "currency"

def get_industry_table_name():
    return "industry"

def get_industry_worldscope_table_name():
    return "industry_worldscope"

def get_master_ohlcvtr_table_name():
    return "master_ohlcvtr"

def get_master_tac_table_name():
    return "master_tac"

def get_master_multiple_table_name():
    return "master_multiple"

def get_report_datapoint_table_name():
    return "report_datapoint"

def get_vix_table_name():
    return "vix"

def get_data_dss_table_name():
    return "data_dss"

def get_data_dsws_table_name():
    return "data_dsws"

def get_data_vix_table_name():
    return "data_vix"

def get_data_dividend_table_name():
    return "data_dividend"

def get_data_dividend_daily_table_name():
    return "data_dividend_daily_rates"

def get_data_interest_table_name():
    return "data_interest"

def get_data_interest_daily_table_name():
    return "data_interest_daily_rates"

def get_latest_price_table_name():
    return "latest_price"

def get_quandl_table_name():
    return "data_quandl"

def get_fundamental_score_table_name():
    return "data_fundamental_score"

def get_data_fred_table_name():
    return "data_fred"

def get_data_ibes_table_name():
    return "data_ibes"

def get_data_ibes_monthly_table_name():
    return "data_ibes_monthly"

def get_data_macro_table_name():
    return "data_macro"

def get_data_macro_monthly_table_name():
    return "data_macro_monthly"

def get_top_stock_models_table_name():
    return "top_stock_models"

def get_top_stock_models_stock_table_name():
    return "top_stock_models_stock"

#Bot Backtest Table
def get_bot_uno_backtest_table_name():
    return "bot_uno_backtest"

def get_bot_ucdc_backtest_table_name():
    return "bot_ucdc_backtest"

def get_bot_classic_backtest_table_name():
    return "bot_classic_backtest"

def get_bot_ranking_table_name():
    return "bot_ranking"

def get_bot_latest_ranking_table_name():
    return "bot_latest_ranking"

def get_bot_data_table_name():
    return "bot_data"

def get_data_vol_surface_table_name():
    return "data_vol_surface"

def get_data_vol_surface_inferred_table_name():
    return "data_vol_surface_inferred"

# from appschema.universe.models import Universe, Currency
# from appschema.datasource.models import ReportDatapoint, MasterOhlcvtr

# universe_table = Universe._meta.db_table
# currency_table = Currency._meta.db_table
# report_datapoint_table = ReportDatapoint._meta.db_table
# master_ohlcvtr_table = MasterOhlcvtr._meta.db_table
report_datapoint_table = ""