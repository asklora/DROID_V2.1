import json
from pymongo import MongoClient
from django.conf import settings
from django.core.cache import cache


mongo_url = 'mongodb+srv://postgres:postgres@cluster0.b0com.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'


if __name__ == "__main__":
    univ = DroidUniverse.objects.filter(is_active=True)[:3]
    bot = BotOptionTypes.objects.all()
    for instance in univ:
        for attr in bot:
            query = BotBacktest.objects.filter(
                            ticker=instance.ticker, 
                            month_to_exp=attr.bot_horizon_month, 
                            bot_type=attr.bot_group, 
                            option_type=attr.bot_option_type).order_by('ticker', 'spot_date')
            value = BotBacktestSerializer(query,many=True).data
            key = f'{instance.ticker}:{attr.bot_horizon_month}:{attr.bot_group}:{attr.bot_option_type}'
            value = json.dumps(value)
            cache.set(key, value)
            print(key)