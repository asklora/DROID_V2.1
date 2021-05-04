from datetime import datetime
from django.core.management.base import BaseCommand
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
from core.orders.models import OrderPosition


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            '-c', '--celery', action='store_true', help='for celery')

    def handle(self, *args, **options):
        positions = OrderPosition.objects.filter(is_live=True)
        for position in positions:
            position_uid = position.position_uid
            if (position.bot.is_uno()):
                if options['celery']:
                    status = uno_position_check.apply_async(
                        args=(position_uid,), queue='ec2')
                else:
                    status = uno_position_check(position_uid)
            elif (position.bot.is_ucdc()):
                if options['celery']:
                    status = ucdc_position_check.apply_async(
                        args=(position_uid,), queue='ec2')
                else:
                    status = ucdc_position_check(position_uid)
            elif (position.bot.is_classic()):
                if options['celery']:
                    status = classic_position_check.apply_async(
                        args=(position_uid,), queue='ec2')
                else:
                    status = classic_position_check(position_uid)
            print(status)
