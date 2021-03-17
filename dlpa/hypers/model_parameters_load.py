from pandas.tseries.offsets import Week, BDay
from general.sql_query import read_query
from general.table_name import get_top_stock_models_table_name
from global_vars import no_top_models

def download_model_data(forward_date, data_period=0):
    # This function is used for downloading the latest available models data for the desired period.
    # E.g. takes the top 10(no_top_models) model and their properties based on their best_valid_acc.
    if data_period == 0:
        period = "weekly"
    else:
        period = "daily"

    table_name = get_top_stock_models_table_name()
    query = f"select * from {table_name} "
    query += f"where forward_date = (select max(forward_date) from {table_name} "
    query += f"where forward_date <= {str(forward_date)} ) and data_period = {period} "
    data = read_query(query, table_name, cpu_counts=True)
    data = data.sort_values(by=["best_valid_acc"], ascending=False)
    data = data.head(no_top_models)
    return data


def load_model_data(forward_date, valid_num, test_num, temp_data, data_period, model_type):
    # Takes the loaded model data from AWS (which is in args.temp)
    # and save them to each args parameter for going into dataset preparation and models inference.
    temp_dict = vars()
    for items in temp_data.iteritems():
        temp_dict[items[0]] = items[1]
    train_num = 0
    if data_period == "weekly":
        data_period = 0
    else:
        data_period = 1

    if model_type == "DLPA":
        model_type = 0
    elif model_type == "DLPM":
        model_type = 1
    else:
        model_type = 2

    production_output_flag = True
    if data_period == 0:
        # We need to add 1 business day to the start since the week starts from it. e.g. test -> Fri => start-> Mon
        start_date = forward_date - Week(train_num + valid_num + 1) + BDay(1)
        end_date = forward_date + Week(test_num - 1)
    else:
        start_date = forward_date - BDay(train_num + valid_num + 1)
        end_date = forward_date + BDay(test_num)

    stage = 0

    return temp_dict, train_num, data_period, model_type, production_output_flag, start_date, end_date, stage
