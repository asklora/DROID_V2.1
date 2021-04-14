from django.db.models.signals import post_save,pre_save
from django.dispatch import receiver
from .models import Order,OrderPosition,PositionPerformance
from core.bot.models import BotOptionType
from bot.calculate_bot import get_classic,get_expiry_date,get_uno,get_ucdc
from datetime import datetime
from core import services
from core.djangomodule.serializers import (OrderSerializer,
OrderPositionSerializer,
PositionPerformanceSerializer)



@receiver(pre_save, sender=Order)
def order_signal_check(sender, instance, **kwargs):
    if instance.placed:
        instance.placed_at =datetime.now()
        if instance.setup:
            if instance.setup['share_num'] == 0:
                instance.status = 'allocated'
            else:
                instance.status = 'pending'
        else:
            instance.status = 'pending'

@receiver(post_save, sender=Order)
def order_signal(sender, instance, created, **kwargs):
    if created and instance.is_init:
        # if bot will create setup expiry , SL and TP
        if instance.bot_id != 'stock':
            bot = BotOptionType.objects.get(bot_id=instance.bot_id)
            expiry = get_expiry_date(bot.time_to_exp,instance.created,instance.symbol.currency_code.currency_code)
            if bot.bot_type.bot_type == 'CLASSIC':
                setup = get_classic(instance.symbol.ticker,instance.created,bot.time_to_exp,instance.amount,instance.price,expiry)
            elif bot.bot_type.bot_type == 'UNO':
                setup = get_uno(instance.symbol.ticker,instance.symbol.currency_code.currency_code, expiry,
                                instance.created,bot.time_to_exp,instance.amount,instance.price,bot.bot_option_type,bot.bot_type.bot_type)
            elif bot.bot_type.bot_type == 'UCDC':
                setup = get_ucdc(instance.symbol.ticker,instance.symbol.currency_code.currency_code, expiry,
                                instance.created,bot.time_to_exp,instance.amount,instance.price,bot.bot_option_type,bot.bot_type.bot_type)
            instance.setup = setup
            instance.save()
            instanceserialize = OrderSerializer(instance).data
            print(instanceserialize)
            data ={
            'type':'function',
            'module':'core.djangomodule.crudlib.order.sync_order',
            'payload':dict(instanceserialize)
            }
            services.celery_app.send_task('config.celery.listener',args=(data,),queue='asklora')
        # if not user can create the setup TP and SL
    elif created and not instance.is_init and instance.bot_id != 'stock':
        pass
    
    # update the status and create position
    elif not created and instance.status in ['filled','allocated']:
        if instance.is_init:
            if instance.status == 'filled':
                spot_date =instance.filled_at.date()
            elif instance.status == 'allocated':
                spot_date =instance.placed_at.date()
            order = OrderPosition.objects.create(
                user=instance.user,
                symbol=instance.symbol,
                bot_id=instance.bot_id,
                spot_date=spot_date,
                entry_price=instance.price,
                investment_amount=instance.amount,
                is_live=True
            )
            perf = PositionPerformance.objects.create(
                position=order,
                last_spot_price=instance.price,
                last_live_price=instance.price
            )
            if instance.setup:
                for key,val in instance.setup.items():
                    if hasattr(order,key):
                        setattr(order,key,val)
                    if hasattr(perf,key):
                        setattr(perf,key,val)
                order.save()
                perf.current_pnl_amt = 0
                perf.current_bot_cash_balance =order.bot_cash_balance
                perf.current_pnl_ret = (perf.current_pnl_amt + perf.current_bot_cash_balance) / order.investment_amount
                perf.current_investment_amount = perf.last_live_price * perf.share_num
                perf.order_id = instance
                perf.save()
            
        else:
            if instance.bot_id != 'stock':
                signal = PositionPerformance.objects.get(id=int(instance.signal_id))
                signal.order_id = instance.uid
                signal.save()
        
        
            