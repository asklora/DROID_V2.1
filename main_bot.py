from general.sql_process import do_function
from main_executive import data_prep_history, infer_history, option_maker_history_classic, train_model

if __name__ == "__main__":
    # do_function("data_vol_surface_update")
    # data_prep_history()
    # train_model()
    # infer_history()
    option_maker_history_classic(ticker=None, currency_code=None, time_to_exp=None, mod=False, option_maker=False, null_filler=False)