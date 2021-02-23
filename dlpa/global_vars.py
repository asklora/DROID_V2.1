import platform
from pathlib import Path
from general.sql_process import db_read, db_write
from general.table_name import (
    get_universe_table_name,
    get_master_multiple_table_name,
    get_master_ohlcvtr_table_name,
    get_master_tac_table_name,
    get_top_stock_models_table_name,
    get_top_stock_models_stock_table_name)
# This file defines the global variables that can be used anywhere in the project

# Databases" URL
DB_URL_READ = db_read
DB_URL_WRITE = db_write

# Databases" tables names
production_model_data_table_name = get_top_stock_models_table_name()
production_inference_table_name = get_top_stock_models_stock_table_name()
production_portfolio_table_name = "client_portfolios"
latest_universe_table_name = get_universe_table_name()
main_input_data_table_name = get_master_multiple_table_name()
client_table_name = "client_group"
master_ohlcv_table_name = get_master_ohlcvtr_table_name()

test_model_data_table_name = "test_model_data"
test_inference_table_name = "test_model_stock_data"
test_portfolio_table_name = "test_portfolios"

num_bins = 3
epoch = 25
# Portfolio parameters
no_top_models = 10
signal_threshold = 0.5
# aws parameters
aws_columns_list = ["model_type", "data_period", "created", "forward_date", "forward_week", "forward_dow",
                    "train_dow", "best_train_acc",
                    "best_valid_acc", "test_acc_1", "test_acc_2", "test_acc_3", "test_acc_4",
                    "test_acc_5", "run_time_min", "train_num", "cnn_kernel_size",
                    "batch_size", "learning_rate", "lookback",
                    "epoch", "param_name_1", "param_val_1", "param_name_2", "param_val_2", "param_name_3",
                    "param_val_3", "param_name_4", "param_val_4", "param_name_5", "param_val_5",
                    "num_bins", "num_nans_to_skip", "accuracy_for_embedding",
                    "candle_type_returnsX", "candle_type_returnsY", "candle_type_candles", "seed",
                    "best_valid_epoch", "best_train_epoch", "model_filename",
                    "pc_number", "stock_percentage", "valid_num", "test_num", "num_periods_to_predict"]

if platform.system() == 'Linux':
    model_path = '/home/loratech/PycharmProjects/models/'
    plot_path = '/home/loratech/PycharmProjects/plots/'
    model_path_clustering = '/home/loratech/PycharmProjects/models/clustering/'
else:
    model_path = 'C:/dlpa_master/model/'
    plot_path = 'C:/dlpa_master/plots/'
    model_path_clustering = 'C:/dlpa_master/model/clustering/'