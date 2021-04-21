from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from .models import Order, OrderPosition, PositionPerformance
from core.bot.models import BotOptionType
from bot.calculate_bot import get_classic, get_expiry_date, get_uno, get_ucdc
from datetime import datetime
from core import services
from core.djangomodule.serializers import (OrderSerializer,
                                           OrderPositionSerializer,
                                           PositionPerformanceSerializer)
import pandas as pd


@receiver(pre_save, sender=Order)
def order_signal_check(sender, instance, **kwargs):
    if instance.placed:
        # instance.placed_at =datetime.now()
        if instance.setup and instance.is_init:
            if instance.setup['share_num'] == 0:
                instance.status = 'allocated'
            elif instance.status == 'filled':
                instance.status = 'filled'
                # instance.filled_at = datetime.now()
            else:
                instance.status = 'pending'


@receiver(post_save, sender=Order)
def order_signal(sender, instance, created, **kwargs):
    if created and instance.is_init:
        # if bot will create setup expiry , SL and TP
        if instance.bot_id != 'stock':
            bot = BotOptionType.objects.get(bot_id=instance.bot_id)
            expiry = get_expiry_date(
                bot.time_to_exp, instance.created, instance.ticker.currency_code.currency_code)
            if bot.bot_type.bot_type == 'CLASSIC':
                setup = get_classic(instance.ticker.ticker, instance.created,
                                    bot.time_to_exp, instance.amount, instance.price, expiry)
            elif bot.bot_type.bot_type == 'UNO':
                setup = get_uno(instance.ticker.ticker, instance.ticker.currency_code.currency_code, expiry,
                                instance.created, bot.time_to_exp, instance.amount, instance.price, bot.bot_option_type, bot.bot_type.bot_type)
            elif bot.bot_type.bot_type == 'UCDC':
                setup = get_ucdc(instance.ticker.ticker, instance.ticker.currency_code.currency_code, expiry,
                                 instance.created, bot.time_to_exp, instance.amount, instance.price, bot.bot_option_type, bot.bot_type.bot_type)
            instance.setup = setup
            instance.qty = setup['share_num']
            instance.save()

        # if not user can create the setup TP and SL
    elif created and not instance.is_init and instance.bot_id != 'stock':
        pass

    # update the status and create position
    elif not created and instance.status in ['filled', 'allocated'] and not PositionPerformance.objects.filter(performance_uid=instance.performance_uid).exists():
        if instance.is_init:
            if instance.status == 'filled':
                spot_date = instance.filled_at
            elif instance.status == 'allocated':
                spot_date = instance.placed_at
            order = OrderPosition.objects.create(
                user_id=instance.user_id,
                ticker=instance.ticker,
                bot_id=instance.bot_id,
                spot_date=spot_date.date(),
                entry_price=instance.price,
                investment_amount=instance.amount,
                is_live=True
            )
            perf = PositionPerformance.objects.create(
                created=pd.Timestamp(order.spot_date),
                position_uid=order,
                last_spot_price=instance.price,
                last_live_price=instance.price,
                order_uid=instance
            )
            if instance.setup:
                for key, val in instance.setup.items():
                    if hasattr(order, key):
                        setattr(order, key, val)
                    if hasattr(perf, key):
                        setattr(perf, key, val)
                
                orderserialize = OrderPositionSerializer(order).data
                orderdata = {
                    'type': 'function',
                    'module': 'core.djangomodule.crudlib.order.sync_position',
                    'payload': dict(orderserialize)
                }
                # services.celery_app.send_task('config.celery.listener',args=(orderdata,),queue='asklora',)
                perf.current_pnl_amt = 0
                perf.current_bot_cash_balance = order.bot_cash_balance
                perf.current_pnl_ret = (
                    perf.current_pnl_amt + perf.current_bot_cash_balance) / order.investment_amount
                perf.current_investment_amount = perf.last_live_price * perf.share_num
                perf.order_id = instance
                perf.save()
                order.save()
                perfserialize = PositionPerformanceSerializer(perf).data
                perfdata = {
                    'type': 'function',
                    'module': 'core.djangomodule.crudlib.order.sync_performance',
                    'payload': dict(perfserialize)
                }
                if instance.bot_id != 'stock':
                    instance.performance_uid = perf.performance_uid
                else:
                    instance.performance_uid = 'stock'
                instance.save()
                # services.celery_app.send_task('config.celery.listener',args=(perfdata,),queue='asklora')

        else:
            if instance.bot_id != 'stock':
                signal_performance = PositionPerformance.objects.get(
                    performance_uid=instance.performance_uid)
                signal_performance.order_id = instance
                signal_performance.save()
    instanceserialize = OrderSerializer(instance).data
    data = {
        'type': 'function',
        'module': 'core.djangomodule.crudlib.order.sync_order',
        'payload': dict(instanceserialize)
    }
    # services.celery_app.send_task('config.celery.listener',args=(data,),queue='asklora')


@receiver(post_save, sender=OrderPosition)
def order_position_signal(sender, instance, created, **kwargs):
    instanceserialize = OrderPositionSerializer(instance).data
    data = {
        'type': 'function',
        'module': 'core.djangomodule.crudlib.order.sync_position',
        'payload': dict(instanceserialize)
    }
#     services.celery_app.send_task('config.celery.listener',args=(data,),queue='asklora')


@receiver(post_save, sender=PositionPerformance)
def order_perfromance_signal(sender, instance, created, **kwargs):
    instanceserialize = PositionPerformanceSerializer(instance).data
    data = {
        'type': 'function',
        'module': 'core.djangomodule.crudlib.order.sync_performance',
        'payload': dict(instanceserialize)
    }
#     services.celery_app.send_task('config.celery.listener',args=(data,),queue='asklora',countdown=5)
