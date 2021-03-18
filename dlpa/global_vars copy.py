# This file defines the global variables that can be used anywhere in the project

# Databases' URL
DB_PROD_URL_WRITE = "postgres://postgres:ml2021#LORA@dlp-prod.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
#DB_DROID_URL_WRITE = "postgres://postgres:ml2021#LORA@droid-aurora2-cluster.cluster-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
DB_TEST_URL_WRITE = "postgres://postgres:DLvalue123@hkpolyu.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"

DB_PROD_URL_READ = "postgres://postgres:ml2021#LORA@dlp-prod.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
#DB_DROID_URL_READ = "postgres://postgres:ml2021#LORA@droid-aurora2-cluster.cluster-ro-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
DB_TEST_URL_READ = "postgres://postgres:DLvalue123@hkpolyu.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"

DB_DROID_URL_READ = "postgres://postgres:ml2021#LORA@droid-v1-cluster.cluster-ro-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
DB_DROID_URL_WRITE = "postgres://postgres:ml2021#LORA@droid-v1-cluster.cluster-cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"

# Databases' tables names
production_model_data_table_name = 'production_model_data'
test_model_data_table_name = 'test_model_data'

production_inference_table_name = 'production_model_stock_data'
test_inference_table_name = 'test_model_stock_data'

production_portfolio_table_name = 'client_portfolios'
test_portfolio_table_name = 'test_portfolios'

latest_universe_table_name = 'universe'
main_input_data_table_name = 'master_daily'
client_table_name = 'client_group'
master_ohlcv_table_name = 'master_ohlcv'
test_portfolio_performance_table_name = 'test_portfolio_performance'
arya_uni2_table_name = 'droid_universe'



# Portfolio parameters
no_top_models = 10
signal_threshold = 0.5
# aws parameters
aws_columns_list = ['model_type', 'data_period', 'when_created', 'forward_date', 'forward_week', 'forward_dow',
                    'train_dow', 'best_train_acc',
                    'best_valid_acc', 'test_acc_1', 'test_acc_2', 'test_acc_3', 'test_acc_4',
                    'test_acc_5', 'run_time_min', 'train_num', 'cnn_kernel_size',
                    'batch_size', 'learning_rate', 'lookback',
                    'epoch', 'param_name_1', 'param_val_1', 'param_name_2', 'param_val_2', 'param_name_3',
                    'param_val_3', 'param_name_4', 'param_val_4', 'param_name_5', 'param_val_5',
                    'num_bins', 'num_nans_to_skip', 'accuracy_for_embedding',
                    'candle_type_returnsX', 'candle_type_returnsY', 'candle_type_candles', 'seed',
                    'best_valid_epoch', 'best_train_epoch', 'model_filename',
                    'pc_number', 'stock_percentage', 'valid_num', 'test_num', 'num_periods_to_predict']
