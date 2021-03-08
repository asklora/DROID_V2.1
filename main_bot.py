from general.sql_process import do_function
from main_executive import data_prep_history, infer_history, option_maker_history_classic, train_model

if __name__ == "__main__":
    # do_function("data_vol_surface_update")
    # data_prep_history()
    # train_model()
    # infer_history()
    option_maker_history_classic(option_maker=True, null_filler=False)