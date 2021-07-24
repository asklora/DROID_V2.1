from django.core.management.base import BaseCommand
from main_executive import daily_classic, daily_shcedule_uno_ucdc, train_lebeler_model, train_model
from global_vars import time_to_expiry, bots_list
import gc

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-run_number", "--run_number", help="run_number", type=int, default=0)
        parser.add_argument("-total_no_of_runs", "--total_no_of_runs", help="total_no_of_runs", type=int, default=1)
        parser.add_argument("-time_to_exp", "--time_to_exp", type=str, help="time_to_exp",)
        parser.add_argument("-currency_code", "--currency_code", nargs="+", help="currency_code", default=None)
        parser.add_argument("-uno", "--uno", type=bool, help="uno", default=False)
        parser.add_argument("-ucdc", "--ucdc", type=bool, help="ucdc", default=False)
        parser.add_argument("-classic", "--classic", type=bool, help="classic", default=False)
        parser.add_argument("-training", "--training", type=bool, help="training", default=False)
        parser.add_argument("-do_infer", "--do_infer", type=bool, help="do_infer", default=False)
        parser.add_argument("-infer", "--infer", type=bool, help="infer", default=False)
        parser.add_argument("-option_maker", "--option_maker", type=bool, help="option_maker", default=False)
        parser.add_argument("-null_filler", "--null_filler", type=bool, help="null_filler", default=False)
        parser.add_argument("-statistic", "--statistic", type=bool, help="statistic", default=False)
        parser.add_argument("-prep", "--prep", type=bool, help="prep", default=False)
        parser.add_argument("-latest_data", "--latest_data", type=bool, help="latest_data", default=False)
        parser.add_argument("-ranking", "--ranking", type=bool, help="ranking", default=False)
        parser.add_argument("-backtest", "--backtest", type=bool, help="backtest", default=False)
    def handle(self, *args, **options):
        if(options["training"]):
            train_model()
            gc.collect()
            for time_to_exp in time_to_expiry:
                for bot in bots_list:
                    train_lebeler_model(time_to_exp=[time_to_exp], bots_list=[bot])
                    gc.collect()

        if(options["classic"]):
            daily_classic()
        else:
            daily_shcedule_uno_ucdc(currency_code=options["currency_code"], uno=options["uno"], 
            ucdc=options["ucdc"], do_infer=options["do_infer"], infer=options["infer"], option_maker=options["option_maker"], 
            null_filler=options["null_filler"], statistic=options["statistic"], prep=options["prep"], 
            latest_data=options["latest_data"], ranking=options["ranking"], backtest=options["backtest"])

# python manage.py backtest --classic True

# python manage.py backtest --prep True --do_infer True
# python manage.py backtest --option_maker True --null_filler True --currency_code USD --uno True --infer True
# python manage.py backtest --option_maker True --null_filler True --currency_code USD --ucdc True --infer True
# python manage.py backtest --backtest True --ranking True --currency_code USD
# python manage.py backtest --statistic True