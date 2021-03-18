import sys
from dlpa.portfolio.client import get_client_information
from dlpa.portfolio.main_file import portfolio_maker, get_dates_list_from_aws, record_DLP_rating, dlp_rating_history
from global_vars import num_periods_to_predict, portfolio_period

def main_process(forward_date_start=None, forward_date_stop=None, num_periods_to_predict = num_periods_to_predict, live=False, future=False):
    client_df = get_client_information()
    stock_num_list = client_df.top_x.unique()

    if live:
        if future:
            portfolio_period = 0
            record_DLP_rating(portfolio_period, num_periods_to_predict, live=live)
        else:
            for num in stock_num_list:
                stock_num = num
                index_list = client_df[client_df.top_x == num]['index_choice_id'].tolist()
                top_buy, top_hold, top_sell = portfolio_maker()
    else:
        if forward_date_start is None and forward_date_stop is None:
            sys.exit('Please input either of the forward_date_start or forward_date_stop!')
        elif forward_date_start is None:
            forward_date_start = forward_date_stop
        elif forward_date_stop is None:
            forward_date_stop = forward_date_start

        dates_list = get_dates_list_from_aws(forward_date_start, forward_date_stop, portfolio_period=portfolio_period)

        for ddate in dates_list:
            forward_date = ddate
            spot_date = None

            forward_date_temp = forward_date
            spot_date_temp = spot_date

            for num in stock_num_list:
                stock_num = num
                index_list = client_df[client_df.top_x == num]['index_choice_id'].tolist()
                top_buy, top_hold, top_sell = portfolio_maker()
                forward_date = forward_date_temp

def live_future_predict4():
    num_periods_to_predict = 4
    main_process(num_periods_to_predict = num_periods_to_predict, live=True, future=True)
    
def live_future_predict13():
    num_periods_to_predict = 13
    main_process(num_periods_to_predict = num_periods_to_predict, live=True, future=True)

if __name__ == "__main__":
    print("Process Here")