from django.core.management.base import BaseCommand
from bot.factory.BaseFactory import BotFactory
from bot.factory.validator import BotCreateProps
from datetime import datetime
# from bot.factory.estimator import BlackScholes
from datasource.rkd import RkdData
from core.djangomodule.general import jsonprint
import random
class Command(BaseCommand):
    def handle(self, *args, **options):
        botIds = ['UNO_OTM_003846','UNO_ITM_003846','UCDC_ATM_008333','CLASSIC_classic_015384']
        propscreate=[]
        for bot_id in botIds:
            props = BotCreateProps(
                ticker="1211.HK",
                spot_date=datetime.now(),
                investment_amount=100000,
                price=290.1,
                bot_id=bot_id,
                margin=random.randint(1,2)
                
            )
            propscreate.append(props)
        factory = BotFactory()
        
        creator = factory.get_batch_creator(propscreate)
        # print(creator.props.bot.bot_type)
        # creator.set_estimator(BlackScholes)
        res = creator.create()
        jsonprint(res.get_result_as_dict())
        print('')
        print('')
            
        # rkd = RkdData()
        # print(rkd.bulk_get_quote(['WYNN.O'],df=True))
