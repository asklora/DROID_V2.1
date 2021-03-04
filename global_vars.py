import os
from dotenv import load_dotenv
load_dotenv()
import time
import platform
from pathlib import Path
# Databases" URL
# DBNAME="postgres"
# DBUSER="postgres"
# DBPASSWORD="ml2021#LORA"
# DROID_DEBUG="False"
# DBHOST="droid-v2-prod-cluster.cluster-cy4dofwtnffp.ap-east-1.rds.amazonaws.com"
# DBHOSTWRITE="droid-v2-prod-cluster.cluster-cy4dofwtnffp.ap-east-1.rds.amazonaws.com"
# DBHOSTREAD="droid-v2-prod-cluster.cluster-ro-cy4dofwtnffp.ap-east-1.rds.amazonaws.com"

DB_URL_READ = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+os.getenv("DBHOSTREAD")+":"+os.getenv("DBPORT")+"/"+os.getenv("DBUSER")
DB_URL_WRITE = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+os.getenv("DBHOSTWRITE")+":"+os.getenv("DBPORT")+"/"+os.getenv("DBUSER")

DBPASSWORDHKPOLYU="DLvalue123"
DBHOSTHKPOLYU="hkpolyu.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com"

DSWS_USERNAME="ZLOL001"
DSWS_PASSWORD="LOTUS239"

DSWS_USERNAME2="ZLOL003"
DSWS_PASSWORD2="YOUNG862"

DSS_USERNAME="9023786"
DSS_PASSWORD="askLORA20$"

SLACK_API="xoxb-305855338628-1139022048576-2KsNu5mJCbgRGh8z8S8NOdGI"
SLACK_CHANNEL="#droid_report"

URL_Extrations="https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Extract"
URL_AuthToken="https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"

URL_APIAsklora="https://api.asklora.ai"
URL_ServiceAsklora="https://services.asklora.ai"

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A1HYBBEY"

QUANDL_URL="https://www.quandl.com/api/v3/datasets/OPT"
QUANDL_KEY="waqWZxLx2dx84mTcsn_w"

REPORT_INTRADAY="Intraday_Pricing"
REPORT_HISTORY="Price_History"

DLP_HISTORY_YEARS=12
DROID_HISTORY_YEARS=4



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
vol_period = 21
classic_business_day = 252
sl_multiplier_1m = -1.25
tp_multiplier_1m = 1.5
sl_multiplier_3m = -1.5
tp_multiplier_3m = 1.75

#BOT TRAINING
bot_labeler_training_num_years = 2
bots_list = ["uno", "ucdc", "classic"]
model_type = "rf"
# if platform.system() == "Linux":
model_path = "/home/loratech/PycharmProjects/models/"
saved_model_path = "/home/loratech/PycharmProjects/DROID/saved_models/"
plot_path = "/home/loratech/PycharmProjects/plots/"
model_path_clustering = "/home/loratech/PycharmProjects/models/clustering/"
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
    "usinter3_esa", "usgbill3_esa", "EMIBOR3._ESA", "jpmshort_esa", "EMGBOND._ESA", "CHGBOND._ESA",
    "fred_data", "eps1fd12", "eps1tr12", "cap1fd12", "fast_d", "fast_k", "rsi"]
    
Y_columns = [["atm_volatility_spot", "atm_volatility_one_year", "atm_volatility_infinity"], ["slope", "deriv_inf", "deriv", "slope_inf"]]