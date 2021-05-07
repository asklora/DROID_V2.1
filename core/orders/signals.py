from core.Clients.models import UserClient
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from core.bot.models import BotOptionType
from bot.calculate_bot import get_classic, get_expiry_date, get_uno, get_ucdc
from datetime import datetime
from core import services
from core.djangomodule.serializers import (OrderSerializer,
                                           OrderPositionSerializer,
                                           PositionPerformanceSerializer)
import pandas as pd
from core.user.models import TransactionHistory


def calculate_fee(amount, side, user_id):
    user_client = UserClient.objects.get(user_id=user_id)
    if(side == "sell"):
        commissions = user_client.client.commissions_sell
        stamp_duty = user_client.stamp_duty_sell
    else:
        commissions = user_client.client.commissions_buy
        stamp_duty = user_client.stamp_duty_buy

    if(user_client.client.commissions_type == "percent"):
        commissions_fee = amount * commissions / 100
    else:
        commissions_fee = commissions

    if(user_client.stamp_duty_type == "percent"):
        stamp_duty_fee = amount * stamp_duty / 100
    else:
        stamp_duty_fee = stamp_duty
    total_fee = commissions_fee + stamp_duty_fee
    return commissions_fee, stamp_duty_fee, total_fee


@receiver(pre_save, sender=Order)
def order_signal_check(sender, instance, **kwargs):
    if instance.placed:
        # instance.placed_at =datetime.now()
        if instance.setup and instance.is_init:
            if instance.setup["share_num"] == 0:
                instance.status = "allocated"
            elif instance.status == "filled":
                instance.status = "filled"
                # instance.filled_at = datetime.now()
            else:
                instance.status = "pending"


@receiver(post_save, sender=Order)
def order_signal(sender, instance, created, **kwargs):

    if created and instance.is_init:
        # if bot will create setup expiry , SL and TP
        if instance.bot_id != "stock":
            bot = BotOptionType.objects.get(bot_id=instance.bot_id)
            margin = False
            if instance.user_id.is_large_margin and bot.bot_type.bot_type != "CLASSIC":
                margin = True
            expiry = get_expiry_date(
                bot.time_to_exp, instance.created, instance.ticker.currency_code.currency_code)
            if bot.bot_type.bot_type == "CLASSIC":
                setup = get_classic(instance.ticker.ticker, instance.created,
                                    bot.time_to_exp, instance.amount, instance.price, expiry)
            elif bot.bot_type.bot_type == "UNO":
                setup = get_uno(instance.ticker.ticker, instance.ticker.currency_code.currency_code, expiry,
                                instance.created, bot.time_to_exp, instance.amount, instance.price, bot.bot_option_type, bot.bot_type.bot_type, margin=margin)
            elif bot.bot_type.bot_type == "UCDC":
                setup = get_ucdc(instance.ticker.ticker, instance.ticker.currency_code.currency_code, expiry,
                                 instance.created, bot.time_to_exp, instance.amount, instance.price, bot.bot_option_type, bot.bot_type.bot_type, margin=margin)
            instance.setup = setup
            instance.qty = setup["share_num"]
            instance.save()

        # if not user can create the setup TP and SL
    elif created and not instance.is_init and instance.bot_id != "stock":
        pass

    elif not created and instance.status in "filled" and not PositionPerformance.objects.filter(performance_uid=instance.performance_uid).exists():
        # update the status and create new position
        if instance.is_init:
            bot = BotOptionType.objects.get(bot_id=instance.bot_id)
            if instance.user_id.is_large_margin and bot.bot_type.bot_type != "CLASSIC":
                margin = 1.5
            else:
                margin = 1

            if instance.status == "filled":
                spot_date = instance.filled_at
                order = OrderPosition.objects.create(
                    user_id=instance.user_id,
                    ticker=instance.ticker,
                    bot_id=instance.bot_id,
                    spot_date=spot_date.date(),
                    entry_price=instance.price,
                    investment_amount=instance.amount,
                    is_live=True,
                    margin=margin
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
                    if hasattr(perf, key):
                        setattr(perf, key, val)
                    if hasattr(order, key):
                        if key == "share_num" and instance.user_id.is_large_margin and bot.bot_type.bot_type != "CLASSIC":
                            continue
                        else:
                            setattr(order, key, val)
                    else:
                        if key == "bot_share_num" and instance.user_id.is_large_margin and bot.bot_type.bot_type != "CLASSIC":
                            setattr(order, "share_num", val)
                digits = max(min(5-len(str(int(perf.last_live_price))), 2), -1)
                margin_investment_amount = round(
                    order.investment_amount*order.margin, digits)
                order.margin_value = round(
                    margin_investment_amount - order.investment_amount, digits)
                order.investment_amount = margin_investment_amount
                orderserialize = OrderPositionSerializer(order).data
                orderdata = {
                    "type": "function",
                    "module": "core.djangomodule.crudlib.order.sync_position",
                    "payload": dict(orderserialize)
                }
                # services.celery_app.send_task("config.celery.listener",args=(orderdata,),queue="asklora",)
                perf.current_pnl_amt = 0
                perf.current_bot_cash_balance = order.bot_cash_balance
                perf.current_pnl_ret = (
                    perf.current_pnl_amt + perf.current_bot_cash_balance) / order.investment_amount
                perf.current_investment_amount = round(
                    perf.last_live_price * perf.share_num, digits)
                perf.order_id = instance
                perf.save()
                order.save()
                perfserialize = PositionPerformanceSerializer(perf).data
                perfdata = {
                    "type": "function",
                    "module": "core.djangomodule.crudlib.order.sync_performance",
                    "payload": dict(perfserialize)
                }

                if instance.bot_id != "stock":
                    instance.performance_uid = perf.performance_uid
                else:
                    instance.performance_uid = "stock"
                instance.save()

                commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                    instance.amount, instance.side, instance.user_id)

                TransactionHistory.objects.create(
                    balance_uid=order.user_id.wallet,
                    side="debit",
                    amount=instance.amount,
                    transaction_detail={
                        "description": "bot order",
                        "position": f"{order.position_uid}",
                        "event": "create",
                    },
                )

                OrderFee.objects.create(
                    order_uid=instance,
                    fee_type=f"{instance.side} fee",
                    commissions=commissions_fee,
                    stamp_duty=stamp_duty_fee,
                    total_fee=total_fee
                )

                TransactionHistory.objects.create(
                    balance_uid=order.user_id.wallet,
                    side="debit",
                    amount=total_fee,
                    transaction_detail={
                        "description": f"{instance.side} fee",
                        "position": f"{order.position_uid}",
                        "event": "fee",
                    },
                )

                # services.celery_app.send_task("config.celery.listener",args=(perfdata,),queue="asklora")

        else:
            # hedging daily here
            if instance.bot_id != "stock":
                if instance.setup:
                    # getting existing position from setup
                    order_position = OrderPosition.objects.get(
                        position_uid=instance.setup["position"]["position_uid"])
                    key_list = list(instance.setup["position"])

                    # update the position
                    for key in key_list:
                        if not key in ["created", "updated", "share_num"]:
                            if hasattr(order_position, key):
                                field = order_position._meta.get_field(key)
                                if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                                    raw_key = f"{key}_id"
                                    setattr(
                                        order_position, raw_key, instance.setup["position"].pop(key))
                                else:
                                    setattr(order_position, key,
                                            instance.setup["position"].pop(key))
                     # remove field position_uid, cause it will double
                    instance.setup["performance"].pop("position_uid")
                    signal_performance = PositionPerformance.objects.create(
                        position_uid=order_position,  # replace with instance
                        # the rest is value from setup
                        **instance.setup["performance"]
                    )

                    signal_performance.order_uid = instance
                    order_position.save()
                    signal_performance.save()

                    if not instance.performance_uid:
                        instance.performance_uid = signal_performance.performance_uid
                        instance.save()

                    # bot perform
                    if instance.side == "sell" and order_position.is_live:
                        commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                            instance.amount, "sell", order_position.user_id)
                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="credit",
                            amount=instance.amount,
                            transaction_detail={
                                "description": "bot perform sell",
                                "position": f"{order_position.position_uid}",
                                "event": "return",
                            },
                        )
                        OrderFee.objects.create(
                            order_uid=instance,
                            fee_type=f"{instance.side} fee",
                            commissions=commissions_fee,
                            stamp_duty=stamp_duty_fee,
                            total_fee=total_fee
                        )

                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="debit",
                            amount=total_fee,
                            transaction_detail={
                                "description": f"{instance.side} fee",
                                "position": f"{order_position.position_uid}",
                                "event": "fee",
                            },
                        )
                    elif instance.side == "buy" and order_position.is_live:
                        commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                            instance.amount, "buy", order_position.user_id)
                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="debit",
                            amount=instance.amount,
                            transaction_detail={
                                "description": "bot perform buy",
                                "position": f"{order_position.position_uid}",
                                "event": "return",
                            },
                        )
                        OrderFee.objects.create(
                            order_uid=instance,
                            fee_type=f"{instance.side} fee",
                            commissions=commissions_fee,
                            stamp_duty=stamp_duty_fee,
                            total_fee=total_fee
                        )

                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="debit",
                            amount=total_fee,
                            transaction_detail={
                                "description": f"{instance.side} fee",
                                "position": f"{order_position.position_uid}",
                                "event": "fee",
                            },
                        )
                    # end portfolio / bot
                    if not order_position.is_live:
                        amt = order_position.investment_amount + order_position.final_pnl_amount

                        commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                            amt, "sell", order_position.user_id)
                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="credit",
                            amount=amt,
                            transaction_detail={
                                "description": "bot return",
                                "position": f"{order_position.position_uid}",
                                "event": "return",
                            },
                        )

                        OrderFee.objects.create(
                            order_uid=instance,
                            fee_type=f"{instance.side} fee",
                            commissions=commissions_fee,
                            stamp_duty=stamp_duty_fee,
                            total_fee=total_fee
                        )

                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="debit",
                            amount=total_fee,
                            transaction_detail={
                                "description": f"{instance.side} fee",
                                "position": f"{order_position.position_uid}",
                                "event": "fee",
                            },
                        )

    # send payload to asklora
    instanceserialize = OrderSerializer(instance).data
    data = {
        "type": "function",
        "module": "core.djangomodule.crudlib.order.sync_order",
        "payload": dict(instanceserialize)
    }
    # services.celery_app.send_task("config.celery.listener",args=(data,),queue="asklora")


@receiver(post_save, sender=OrderPosition)
def order_position_signal(sender, instance, created, **kwargs):
    instanceserialize = OrderPositionSerializer(instance).data
    data = {
        "type": "function",
        "module": "core.djangomodule.crudlib.order.sync_position",
        "payload": dict(instanceserialize)
    }
#     services.celery_app.send_task("config.celery.listener",args=(data,),queue="asklora")


@receiver(post_save, sender=PositionPerformance)
def order_perfromance_signal(sender, instance, created, **kwargs):
    instanceserialize = PositionPerformanceSerializer(instance).data
    data = {
        "type": "function",
        "module": "core.djangomodule.crudlib.order.sync_performance",
        "payload": dict(instanceserialize)
    }
#     services.celery_app.send_task("config.celery.listener",args=(data,),queue="asklora",countdown=5)
