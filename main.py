from ingestion.data_from_rkd import update_lot_size_from_rkd
from general.date_process import dateNow
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
    for index, row in client_portfolios_missing.iterrows():
        ticker = row["ticker"]
        spot_date = row["spot_date"]
        forward_date = row["forward_date"]
        print("{} : === This ticker {} is null on {} to {}===".format(
            dateNow(), ticker, spot_date, forward_date))

    # Holiday report
    for index, row in client_portfolios_holiday.iterrows():
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

# Main Process
if __name__ == "__main__":
    print("Start Process")
    currency_code = ["SGD"]
    ticker = ["MSFT.O", "AAPL.O"]
    update_lot_size_from_rkd(ticker=ticker)
