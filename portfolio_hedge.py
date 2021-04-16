from core.bot.models import BotOptionType
from core.orders.models import OrderPosition
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check
import sys
def test():
    positions = OrderPosition.objects.filter(is_live=True, bot_id="CLASSIC_classic_007692")
    for position in positions:
        position_uid = position.position_uid
        if(position.bot.is_uno()):
            status = uno_position_check(position_uid)
        elif(position.bot.is_ucdc()):
            status = ucdc_position_check(position_uid)
        else:
            status = classic_position_check(position_uid)
        print(status)