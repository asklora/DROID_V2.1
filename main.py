from general.date_process import dateNow
from ingestion.data_from_dsws import update_data_dsws_from_dsws
from ingestion.data_from_dss import update_data_dss_from_dss
from general.sql_query import get_active_universe, get_active_universe_droid1, get_universe_by_region
from general.sql_process import do_function
from general.sql_query import read_query

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
    print("Start Process")
    from migrate import weekly_migrations, daily_migrations
    
