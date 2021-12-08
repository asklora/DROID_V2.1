
from django.core.management.base import BaseCommand
from portfolio.factory.BaseFactory import BotFactory
from portfolio.factory.validator import BotCreateProps
from datetime import datetime
from portfolio.factory.estimator import BlackScholes

class Command(BaseCommand):
    def handle(self, *args, **options):
        props = BotCreateProps(
            ticker='1211.HK',
            spot_date=datetime.now(),
            investment_amount=100000,
            price=290.1,
            bot_id = 'UNO_OTM_016666'
        )
        factory = BotFactory()
        creator = factory.get_creator(props)
        creator.set_estimator(BlackScholes)
        creator.create()
       