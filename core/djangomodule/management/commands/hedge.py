from datetime import datetime
from django.core.management.base import BaseCommand
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
from core.orders.models import OrderPosition


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            "-c", "--celery", action="store_true", help="for celery")
        parser.add_argument("-q", "--queue", type=str, help="queue")
        parser.add_argument("-currency", "--currency", type=str, help="currency")

    def handle(self, *args, **options):
        if options['currency']:
            cur = options['currency']
        else:
            cur = 'USD'
        positions = OrderPosition.objects.filter(is_live=True,ticker__currency_code=cur)
        for position in positions:
            position_uid = position.position_uid
            if (position.bot.is_uno()):
                if options["celery"]:
                    status = uno_position_check.apply_async(
                        args=(position_uid,), queue=options["queue"])
                else:
                    status = uno_position_check(position_uid, hedge=True)
            elif (position.bot.is_ucdc()):
                if options["celery"]:
                    status = ucdc_position_check.apply_async(
                        args=(position_uid,), queue=options["queue"])
                else:
                    status = ucdc_position_check(position_uid, hedge=True)
            elif (position.bot.is_classic()):
                if options["celery"]:
                    status = classic_position_check.apply_async(
                        args=(position_uid,), queue=options["queue"])
                else:
                    status = classic_position_check(position_uid, hedge=True)
            print(status, "done")
