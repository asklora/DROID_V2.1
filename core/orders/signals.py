from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Order,OrderPosition,PositionPerformance
from core.bot.models import BotOptionType
from bot.calculate_bot import get_classic,get_expiry_date,get_uno,get_ucdc

@receiver(post_save, sender=Order)
def order_signal(sender, instance, created, **kwargs):
    if created and instance.is_init:
        if instance.bot_id != 'stock':
            bot = BotOptionType.objects.get(bot_id=instance.bot_id)
            expiry = get_expiry_date(bot.time_to_exp,instance.created,instance.symbol.currency_code.currency_code)
            if bot.bot_type.bot_type == 'CLASSIC':
                setup = get_classic(instance.symbol.ticker,instance.created,bot.time_to_exp,instance.amount,instance.price,expiry)
            elif bot.bot_type.bot_type == 'UNO':
                setup = get_uno(instance.symbol.ticker,instance.symbol.currency_code.currency_code, expiry.date(),
                                instance.created.date(),bot.time_to_exp,instance.amount,instance.price,bot.bot_option_type,bot.bot_type.bot_type)
            elif bot.bot_type.bot_type == 'UCDC':
                setup = get_ucdc(instance.symbol.ticker,instance.symbol.currency_code.currency_code, expiry,
                                instance.created,bot.time_to_exp,instance.amount,instance.price,bot.bot_option_type,bot.bot_type.bot_type)
            instance.setup = setup
            instance.save()
            