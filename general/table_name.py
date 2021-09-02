'''
Utility functions to get database table names
'''

def get_region_table_name():
    return "region"

def get_calendar_table_name():
    return "currency_calendar"

def get_universe_consolidated_table_name():
    return "universe_consolidated"

def get_universe_table_name():
    return "universe"

def get_universe_client_table_name():
    return "universe_client"

def get_universe_rating_table_name():
    return "universe_rating"

def get_universe_rating_history_table_name():
    return "universe_rating_history"

def get_universe_rating_detail_history_table_name():
    return "universe_rating_detail_history"

def get_currency_table_name():
    return "currency"

def get_currency_calendar_table_name():
    return "currency_calendar"

def get_industry_table_name():
    return "industry"

def get_industry_group_table_name():
    return "industry_group"

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

def get_data_split_table_name():
    return "data_split"

def get_vix_table_name():
    return "vix"

def get_data_dss_table_name():
    return "data_dss"

def get_data_dsws_table_name():
    return "data_dsws"

def get_data_vix_table_name():
    return "data_vix"

def get_latest_price_table_name():
    return "latest_price"

def get_quandl_table_name():
    return "data_quandl"

def get_fundamental_score_table_name():
    return "data_fundamental_score"

def get_data_fundamental_score_history_table_name():
    return "data_fundamental_score_history"

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

def get_data_worldscope_summary_table_name():
    return "data_worldscope_summary"

def get_ai_value_pred_table_name():
    return "ai_value_lgbm_pred_final_eps"

def get_ai_score_history_testing_table_name():
    return "data_fundamental_score_history_testing"

def get_factor_calculation_table_name():
    return "factor_formula_ratios_prod"

def get_factor_rank_table_name():
    return "factor_result_pred_prod"

def get_top_stock_models_table_name():
    return "top_stock_models"

def get_top_stock_models_stock_table_name():
    return "top_stock_models_stock"

#Bot Backtest Table
def get_bot_backtest_table_name():
    return "bot_backtest"

def get_bot_uno_backtest_table_name():
    return "bot_uno_backtest"

def get_bot_ucdc_backtest_table_name():
    return "bot_ucdc_backtest"

def get_bot_classic_backtest_table_name():
    return "bot_classic_backtest"

def get_bot_ranking_table_name():
    return "bot_ranking"

def get_bot_latest_ranking_table_name():
    return "latest_bot_ranking"

def get_latest_bot_data_table_name():
    return "latest_bot_data"

def get_bot_data_table_name():
    return "bot_data"

def get_data_vol_surface_table_name():
    return "data_vol_surface"

def get_data_vol_surface_inferred_table_name():
    return "data_vol_surface_inferred"

def get_bot_statistic_table_name():
    return "bot_statistic"

def get_client_table_name():
    return "client"

def get_user_clients_table_name():
    return "user_clients"

def get_data_dividend_table_name():
    return "data_dividend"

def get_data_dividend_daily_rates_table_name():
    return "data_dividend_daily_rates"

def get_data_interest_table_name():
    return "data_interest"

def get_data_interest_daily_rates_table_name():
    return "data_interest_daily_rates"

def get_latest_vol_table_name():
    return "latest_vol"

def get_latest_bot_update_table_name():
    return "latest_bot_update"

def get_latest_bot_ranking_table_name():
    return "latest_bot_ranking"

def get_bot_type_table_name():
    return "bot_type"

def get_bot_option_type_table_name():
    return "bot_option_type"

def get_orders_table_name():
    return "orders"

def get_orders_position_table_name():
    return "orders_position"

def get_orders_position_performance_table_name():
    return "orders_position_performance"

def get_client_test_pick_table_name():
    return "client_test_pick"

def get_client_top_stock_table_name():
    return "client_top_stock"

def get_user_core_table_name():
    return "user_core"

def get_user_account_balance_table_name():
    return "user_account_balance"

def get_user_profit_history_table_name():
    return "user_profit_history"
    
# from appschema.universe.models import Universe, Currency
# from appschema.datasource.models import ReportDatapoint, MasterOhlcvtr

# universe_table = Universe._meta.db_table
# currency_table = Currency._meta.db_table
# report_datapoint_table = ReportDatapoint._meta.db_table
# master_ohlcvtr_table = MasterOhlcvtr._meta.db_table
# report_datapoint_table = ""