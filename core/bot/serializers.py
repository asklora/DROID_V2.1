from pandas.core.tools.datetimes import DatetimeScalar
from bot import calculate_bot
from rest_framework import serializers, exceptions
from core.universe.models import Universe
from .models import BotOptionType
from datetime import datetime


class BotHedgerSerializer(serializers.Serializer):
    bot_id=serializers.CharField(required=True,write_only=True)
    ticker=serializers.CharField(required=True,write_only=True)
    price=serializers.FloatField(required=True,write_only=True)
    amount=serializers.FloatField(required=True,write_only=True)
    expiry=serializers.CharField(read_only=True)
    max_loss_pct=serializers.FloatField(read_only=True)
    max_loss_price=serializers.FloatField(read_only=True)
    max_loss_amount=serializers.FloatField(read_only=True)
    target_profit_pct=serializers.FloatField(read_only=True)
    target_profit_price=serializers.FloatField(read_only=True)
    target_profit_amount=serializers.FloatField(read_only=True)


    def create(self, validated_data):
        print(validated_data)
        try:
            ticker = Universe.objects.get(ticker=validated_data['ticker'])
        except Universe.DoesNotExist:
            raise exceptions.NotFound({'detail':'Ticker not found'})

        try:
            bot = BotOptionType.objects.get(bot_id=validated_data['bot_id'])
        except BotOptionType.DoesNotExist:
            raise exceptions.NotFound({'detail':'Bot not found'})


    
        margin = False
        expiry = calculate_bot.get_expiry_date(
            bot.time_to_exp, datetime.now(), ticker.currency_code.currency_code)
        if bot.bot_type.bot_type == "CLASSIC":
            setup = calculate_bot.get_classic(ticker.ticker, datetime.now(),
                                bot.time_to_exp, validated_data['amount'], validated_data['price'], expiry)
        elif bot.bot_type.bot_type == "UNO":
            setup = calculate_bot.get_uno(ticker.ticker, ticker.currency_code.currency_code, expiry,
                            datetime.now(), bot.time_to_exp, validated_data['amount'], validated_data['price'], bot.bot_option_type, bot.bot_type.bot_type, margin=margin)
        elif bot.bot_type.bot_type == "UCDC":
            setup = calculate_bot.get_ucdc(ticker.ticker, ticker.currency_code.currency_code, expiry,
                                    datetime.now(), bot.time_to_exp, validated_data['amount'], validated_data['price'], bot.bot_option_type, bot.bot_type.bot_type, margin=margin)
        data={}
        data["expiry"]=setup["expiry"]
        data["max_loss_pct"]=setup["max_loss_pct"]
        data["max_loss_price"]=setup["max_loss_price"]
        data["max_loss_amount"]=setup["max_loss_amount"]
        data["target_profit_pct"]=setup["target_profit_pct"]
        data["target_profit_price"]=setup["target_profit_price"]
        data["target_profit_amount"]=setup["target_profit_amount"]
    
        return data   
