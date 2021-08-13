import os
import uuid
from dotenv import load_dotenv
load_dotenv()
import time
import platform
from pathlib import Path
from datetime import datetime
# Databases" URL
# DBNAME="postgres"
# DBUSER="postgres"
# DBPASSWORD="ml2021#LORA"
# DROID_DEBUG="False"
# DBHOST="droid-v2-prod-cluster.cluster-cy4dofwtnffp.ap-east-1.rds.amazonaws.com"
# DBHOSTWRITE="droid-v2-prod-cluster.cluster-cy4dofwtnffp.ap-east-1.rds.amazonaws.com"
# DBHOSTREAD="droid-v2-prod-cluster.cluster-ro-cy4dofwtnffp.ap-east-1.rds.amazonaws.com"

MONGO_URL = "mongodb+srv://postgres:postgres@cluster0.b0com.mongodb.net/test?retryWrites=true&w=majority"

DB_URL_READ = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+os.getenv("DBHOSTREAD")+":"+os.getenv("DBPORT")+"/"+os.getenv("DBUSER")
DB_URL_WRITE = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+os.getenv("DBHOSTWRITE")+":"+os.getenv("DBPORT")+"/"+os.getenv("DBUSER")
DB_URL_ALIBABA = "postgres://loratechai:AskLORAv2@pgm-3nse9b275d7vr3u18o.pg.rds.aliyuncs.com:1921/postgres"

DBPASSWORDHKPOLYU="DLvalue123"
DBHOSTHKPOLYU="hkpolyu.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com"

DSWS_USERNAME="ZLOL001"
DSWS_PASSWORD="LOTUS239"

DSWS_USERNAME2="ZLOL003"
DSWS_PASSWORD2="YOUNG862"

# DSS_USERNAME = "9029345" # Corporate Action
# DSS_PASSWORD = "AskLORA2" 

DSS_USERNAME="9023786"
DSS_PASSWORD="AskLORAv2" #CHANGED Password

# SLACK_API="xoxb-305855338628-1139022048576-2KsNu5mJCbgRGh8z8S8NOdGI"
# SLACK_CHANNEL="#droid_report"

SLACK_API="xoxb-305855338628-1139022048576-2KsNu5mJCbgRGh8z8S8NOdGI"
SLACK_CHANNEL="#droid_v2_report"
SLACK_TEST_CHANNEL="#droid_v2_test_report"

URL_Extrations="https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/Extract"
URL_Extrations_with_note="https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ExtractWithNotes"
URL_AuthToken="https://selectapi.datascope.refinitiv.com/RestApi/v1/Authentication/RequestToken"

URL_APIAsklora="https://api.asklora.ai"
URL_ServiceAsklora="https://services.asklora.ai"

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A1HYBBEY"

QUANDL_URL="https://www.quandl.com/api/v3/datasets/OPT"
QUANDL_KEY="waqWZxLx2dx84mTcsn_w"

REPORT_INDEXMEMBER="Index_Member"
REPORT_EOD="End_Of_Day"
REPORT_INTRADAY="Intraday_Pricing"
REPORT_HISTORY="Price_History"

DLP_HISTORY_YEARS=12
DROID_HISTORY_YEARS=4

#vol ranges
max_vol = 0.95
min_vol = 0.2
default_vol = 0.25

#Hedging
large_hedge = 0.05
small_hedge = 0.02
buy_UCDC_prem = 1.005
sell_UCDC_prem = 0.995
buy_UNO_prem = 1.015
sell_UNO_prem = 0.99

# BOT BACKTEST VARIABEL
null_per = 5
period = 21
random_state = 8
# valid_size = 
r_days=200
q_days=200
currency_code_to_etf_file = "files/file_csv/currency_code_to_etf.csv"
run_time_min = time.time()
modified_delta_list = ["d2", "v10"]
time_to_expiry = [0.03846, 0.07692, 0.08333, 0.15384, 0.16666, 0.25, 0.5] #[2 weeks, 4 weeks, 1 month, 8 weeks, 2 months, 3 months, 6 months]
statistics_lookback_list = [6, 12, 36]
vol_period = 21
classic_business_day = 252
sl_multiplier_1m = -1.25
tp_multiplier_1m = 1.5
sl_multiplier_3m = -1.5
tp_multiplier_3m = 1.75

#BOT TRAINING
bot_labeler_training_num_years = 2
bot_labeler_threshold = 0.02 # threshold to be deemed "profitable" for a bot
bot_slippage = 0.0025 # comms + fees + slippage each way for bots
bots_list = ["uno", "ucdc", "classic"]
labeler_model_type = "rf"

model_path = "/media/loratech/c6a1535c-380b-4d1f-a9bf-56d8fe0327581/PycharmProjects/DROID_V2.1/models/"
saved_model_path = "/media/loratech/c6a1535c-380b-4d1f-a9bf-56d8fe0327581/PycharmProjects/DROID_V2.1/saved_models/"
plot_path = "/media/loratech/c6a1535c-380b-4d1f-a9bf-56d8fe0327581/PycharmProjects/DROID_V2.1/plots/"
model_path_clustering = "/media/loratech/c6a1535c-380b-4d1f-a9bf-56d8fe0327581/PycharmProjects/DROID_V2.1/clustering/"

# else:
#     model_path = "C:/dlpa_master/model/"
#     saved_model_path = "C:/dlpa_master/model/saved_models/"
#     plot_path = "C:/dlpa_master/plots/"
#     model_path_clustering = "C:/dlpa_master/model/clustering/"

model_filename = [saved_model_path + "vols_finalized_model.joblib", "_finalized_model.txt"]

X_columns = ["kurt_0_504", "vix_value",
    "c2c_vol_0_21", "c2c_vol_21_42", "c2c_vol_42_63", "c2c_vol_63_126", "c2c_vol_126_252", "c2c_vol_252_504", 
    "rs_vol_0_21", "rs_vol_21_42", "rs_vol_42_63", "rs_vol_63_126", "rs_vol_126_252", "rs_vol_252_504", 
    "total_returns_0_1", "total_returns_0_21", "total_returns_0_63", "total_returns_21_126", "total_returns_21_231",
    "atm_volatility_spot_x", "atm_volatility_one_year_x", "atm_volatility_infinity_x", 
    "total_returns_0_21_x", "total_returns_0_63_x", "total_returns_21_126_x", "total_returns_21_231_x", 
    "c2c_vol_0_21_x", "c2c_vol_21_42_x", "c2c_vol_42_63_x", "c2c_vol_63_126_x", "c2c_vol_126_252_x", "c2c_vol_252_504_x", 
    "usinter3", "usgbill3", "emibor3", "jpmshort", "emgbond", "chgbond",
    "fred_data", "eps1fd12", "eps1tr12", "cap1fd12", "fast_d", "fast_k", "rsi"]
    
Y_columns = [["atm_volatility_spot", "atm_volatility_one_year", "atm_volatility_infinity"], ["slope", "deriv_inf", "deriv", "slope_inf"]]

def folder_check(path=saved_model_path):
    if platform.system() == "Linux":
        if not os.path.exists(path):
            os.makedirs(path)


#DLPA
EC2_hostname="13.209.141.35"
EC2_username="seoul"
EC2_model_key_file="dlpa/extra/model_saving_key.pem"
no_top_models = 10
signal_threshold = 0.5
candle_type_candles = 0 # this should DEFAULT TO 0 - use all candles
candle_type_returnsY = 4 # this should DEFAULT TO 4 - use close
candle_type_returnsX = 4 # this should DEFAULT TO 4 - use close
pickle_update = False
model_type=1  # use DLPM
epoch=25  # 25 epochs seem enough (more takes too much time)
Hyperopt_runs=10 # don"t need more than 20
forward_year=0
forward_week=1 #v2 default = 1
forward_dayt=5  #v2 default = 5
dow=5
data_period=0  # the period to forecast - one week ahead of one day ahead?
stage=0  # not going to explore other stages yet
stock_percentage=1.0
train_num=390 # number of periods to train before test period (and valid periods if >0) 390 for weekly 1250 for daily
valid_num=0 # valid_num = 0 => random shuffle 20%, else chronological periods before test period
test_num=1  # test_num should BE 1.
period_jump=1 # JUMP <>1 for model tuning and sampling - should go to 1 for prod
update_lookback=2 # how many EXTRA periods to update?
num_periods_to_predict=1 # needs to be >1 for --future
num_bins=3
num_nans_to_skip=1# Nan FILTERING
seed=123
best_train_acc=1e-1
best_valid_acc=1e-1
test_acc_1=0
test_acc_2=0
test_acc_3=0
test_acc_4=0
test_acc_5=0
lowest_train_loss=1e-1
lowest_valid_loss=1e-1
best_valid_epoch=1
lowest_train_epoch=1
unique_num_of_returns=1
unique_num_of_outputs=1
timestamp=time.time()
gpu_number=0
pc_number="unknown"
# db_end_date=datetime.today() - BDay(2)

aws_columns_list = ["model_type", "data_period", "when_created", "forward_date", "forward_week", "forward_dow",
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


#WTS Poprtfolio
# 0 -> weekly,
# 1 -> daily,
# 2 -> Point in time (PIT),
portfolio_period=0
client_name="LORATECH"
signal_threshold = 0.5
seed=123
mode="client"
port=64891
num_periods_to_predict=1
