from main import daily_process_ohlcvtr
from django.core.management.base import BaseCommand
import gc

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-na", "--na", type=bool, help="north_asia", default=False)
        parser.add_argument("-ws", "--ws", type=bool, help="west", default=False)
        # parser.add_argument("-classic", "--classic", type=bool, help="classic", default=False)
        # parser.add_argument("-training", "--training", type=bool, help="training", default=False)
        # parser.add_argument("-do_infer", "--do_infer", type=bool, help="do_infer", default=False)
        # parser.add_argument("-infer", "--infer", type=bool, help="infer", default=False)
        # parser.add_argument("-option_maker", "--option_maker", type=bool, help="option_maker", default=False)
        # parser.add_argument("-null_filler", "--null_filler", type=bool, help="null_filler", default=False)
        # parser.add_argument("-statistic", "--statistic", type=bool, help="statistic", default=False)
        # parser.add_argument("-prep", "--prep", type=bool, help="prep", default=False)
        # parser.add_argument("-latest_data", "--latest_data", type=bool, help="latest_data", default=False)
        # parser.add_argument("-ranking", "--ranking", type=bool, help="ranking", default=False)
        # parser.add_argument("-backtest", "--backtest", type=bool, help="backtest", default=False)
    def handle(self, *args, **options):
        if(options["na"]):
            daily_process_ohlcvtr(region_id=["na"])
        elif(options["ws"]):
            daily_process_ohlcvtr(region_id=["ws"])