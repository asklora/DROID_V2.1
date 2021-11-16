from .models import OrderPosition,Order
from core.bot.models import BotOptionType
from typing import Union
class OrderPositionValidation:
    def __init__(self,ticker:str,bot_id:Union[str,list,None],user_id:Union[str,int]):
        self.ticker =ticker
        self.bot_id =bot_id
        self.user_id =user_id
        if isinstance(bot_id,str):
            bot_type = BotOptionType.objects.get(bot_id=bot_id).bot_type
            self.bots = [bot['bot_id'] for bot in BotOptionType.objects.filter(bot_type=bot_type).values('bot_id')]
        elif isinstance(bot_id,list):
            self.bots= self.bot_id+["STOCK_stock_0"]
            

    def allowed_bot(self):
        
        portfolios = OrderPosition.objects.filter(user_id=self.user_id,ticker=self.ticker,is_live=True).prefetch_related('ticker').distinct('bot_id')
        orders = Order.objects.filter(user_id=self.user_id,ticker=self.ticker,status='pending',side='buy').distinct('bot_id')
        bot_portfolios = [bot.bot_id for bot in portfolios]
        bot_orders = [bot.bot_id for bot in orders]
        used_bot = bot_portfolios+bot_orders
        used_bot_type = BotOptionType.objects.filter(bot_id__in=used_bot).distinct('bot_type').values('bot_type')
        unused_bot_list = [bot['bot_id']for bot in BotOptionType.objects.exclude(bot_type__in=used_bot_type).values('bot_id')]
        response =[]
        for unused in self.bots:
            data ={}
            if  unused in unused_bot_list:
                data[unused] = True
            else:
                data[unused]=False
            response.append(data)
        return {"allowed_bot":response}