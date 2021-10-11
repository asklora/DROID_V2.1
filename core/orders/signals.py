from core.universe.models import Currency
from core.user.convert import ConvertMoney
import math
from django.db.models.signals import post_save, pre_save, post_delete
from django.dispatch import receiver
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from core.bot.models import BotOptionType
from bot.calculate_bot import get_classic, get_expiry_date, get_uno, get_ucdc
from core.djangomodule.general import formatdigit
from core.user.models import TransactionHistory
from .order_signal_action import OrderServices
from django.utils import timezone

def generate_hedge_setup(instance: Order,margin:int) -> dict:

    bot = BotOptionType.objects.get(bot_id=instance.bot_id)
    expiry = get_expiry_date(bot.time_to_exp, instance.created, instance.ticker.currency_code.currency_code, apps=(instance.order_type == "apps"))
    if bot.bot_type.bot_type == "CLASSIC":
        setup = get_classic(instance.ticker.ticker, instance.created,
                            bot.time_to_exp, instance.converted_amount, instance.price, expiry,margin=margin)
    elif bot.bot_type.bot_type == "UNO":
        setup = get_uno(instance.ticker.ticker, instance.ticker.currency_code.currency_code, expiry,
                        instance.created, bot.time_to_exp, instance.converted_amount, instance.price, bot.bot_option_type, bot.bot_type.bot_type, margin=margin)
    elif bot.bot_type.bot_type == "UCDC":
        setup = get_ucdc(instance.ticker.ticker, instance.ticker.currency_code.currency_code, expiry,
                         instance.created, bot.time_to_exp, instance.converted_amount, instance.price, bot.bot_option_type, bot.bot_type.bot_type, margin=margin)

    return setup


@receiver(pre_save, sender=Order)
def order_signal_check(sender, instance, **kwargs):
    currency_decimal =instance.user_id.user_balance.currency_code.is_decimal
    # this for locking balance before order is filled
    if instance.placed and instance.status == 'placed':
        print("PLACED")
        if instance.status != "filled":
            instance.status = "pending"
            
    # if status not in ["filled", "placed", "pending", "cancel"] and is new order, recalculate price and share
    if not instance.status in ["filled", "placed", "pending", "cancel"] and instance.is_init:
        # if(instance.order_type == "apps"):
        #     from_curr = instance.user_id.user_balance.currency_code
        #     to_curr = instance.ticker.currency_code
        #     convert = ConvertMoney(from_curr, to_curr)
        #     instance.amount = convert.convert(instance.amount)
        # if bot will create setup expiry , SL and TP
        if instance.is_bot_order:
            setup = generate_hedge_setup(instance,instance.margin)
            instance.setup = setup
            instance.qty = setup['performance']["share_num"]
            instance.amount = formatdigit(setup['performance']["share_num"] * setup['price'],currency_decimal)
        else:
            instance.setup = None
            # amount should still
            if not instance.qty:
                instance.qty = math.floor((instance.amount / instance.price) * instance.margin) 
            instance.amount = formatdigit((instance.qty * instance.price) / instance.margin,currency_decimal)

@receiver(post_save, sender=Order)
def order_signal(sender, instance, created, **kwargs):
    services=OrderServices(instance)
    services.process_transaction()
    
    
@receiver(post_delete, sender=PositionPerformance)
def order_revert(sender, instance, **kwargs):
    skip=False
    if instance.order_uid:
        order = Order.objects.get(order_uid=instance.order_uid.order_uid)
        in_wallet_transactions = TransactionHistory.objects.filter(
            transaction_detail__order_uid=str(order.order_uid))
        if in_wallet_transactions.exists():
            in_wallet_transactions.delete()
        try:
            position = OrderPosition.objects.get(
                position_uid=instance.position_uid.position_uid)
        except OrderPosition.DoesNotExist:
            skip=True
        if not skip:
            # return to bot cash balance
            if order.side == 'sell':
                position.bot_cash_balance = position.bot_cash_balance - order.amount
            elif order.side == 'buy':
                position.bot_cash_balance = position.bot_cash_balance + order.amount
            if position.order_position.all().exists():
                if not position.is_live:
                    position.is_live = True
                position.save()
            else:
                position.delete()
        order.delete()

@receiver(post_delete, sender=OrderFee)
def return_fee_to_wallet(sender, instance, **kwargs):
    in_wallet_transactions_fee = TransactionHistory.objects.filter(
        transaction_detail__fee_id=instance.id)
    in_wallet_transactions_stamp = TransactionHistory.objects.filter(
        transaction_detail__stamp_duty_id=instance.id)
    if in_wallet_transactions_fee.exists():
        in_wallet_transactions_fee.delete()
    if in_wallet_transactions_stamp.exists():
        in_wallet_transactions_stamp.delete()
