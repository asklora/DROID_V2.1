from core.bot.models import BotOptionType
from core.orders.models import OrderPosition
from portfolio.daily_hedge_classic import classic_position_check
from portfolio.daily_hedge_ucdc import ucdc_position_check
from portfolio.daily_hedge_uno import uno_position_check

def test():
    positions = OrderPosition.objects.filter(is_live=True)
    for position in positions():
        position_uid = position.position_uid
        bot = BotOptionType.objects.get(bot_id = position.bot_id)
        if(bot.bot_type == "UNO"):
            status = uno_position_check(position_uid)
        elif(bot.bot_type == "UCDC"):
            status = ucdc_position_check(position_uid)
        else:
            status = classic_position_check(position_uid)
        print(status)
        
        