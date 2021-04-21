from core.bot.models import BotOptionType
from core.orders.models import OrderPosition
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
import sys


def test():
    positions = OrderPosition.objects.filter(is_live=True, position_uid="0926a326-4225-4552-8386-af0a4a1ceec8")
    for position in positions:
        position_uid = position.position_uid
        if(position.bot.is_uno()):
            # status = uno_position_check.apply_async(args=(position_uid,), queue='droid')
            status = uno_position_check(position_uid)
        elif(position.bot.is_ucdc()):
            # status = ucdc_position_check.apply_async(
            #     args=(position_uid,), queue='droid')
            status = ucdc_position_check(position_uid)
        elif(position.bot.is_classic()):
            # status = classic_position_check.apply_async(
            #     args=(position_uid,), queue='droid')
            status = classic_position_check(position_uid)
        print(status)
