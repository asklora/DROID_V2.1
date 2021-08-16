import math
from core.Clients.models import UserClient
from django.db.models.signals import post_save, pre_save, post_delete
from django.dispatch import receiver
from .models import Order, OrderFee, OrderPosition, PositionPerformance
from core.bot.models import BotOptionType
from bot.calculate_bot import get_classic, get_expiry_date, get_uno, get_ucdc
from core.djangomodule.general import formatdigit
from core.user.models import TransactionHistory, Accountbalance

"""
bot or not
new position or end position
using broker or not
include fee or not
"""



def update_initial_transaction_position(instance:Order,position_uid:str):
    # update the transaction
    trans = TransactionHistory.objects.filter(side="debit", transaction_detail__description="bot order", transaction_detail__order_uid=str(instance.order_uid))
    if trans.exists():
        trans = trans.get()
        trans.transaction_detail["position"] = position_uid
        trans.save()
    # update fee transaction
    fee_trans = TransactionHistory.objects.filter(side="debit", transaction_detail__event="fee", transaction_detail__order_uid=str(instance.order_uid))
    if fee_trans.exists():
        fee_trans = fee_trans.get()
        fee_trans.transaction_detail["position"] = position_uid
        fee_trans.save()
    # update stamp_duty transaction
    stamp_trans = TransactionHistory.objects.filter(side="debit", transaction_detail__event="stamp_duty", transaction_detail__order_uid=str(instance.order_uid))
    if stamp_trans.exists():
        stamp_trans = stamp_trans.get()
        stamp_trans.transaction_detail["position"] = position_uid
        stamp_trans.save()





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
    return formatdigit(commissions_fee), formatdigit(stamp_duty_fee), formatdigit(total_fee)

def create_fee(instance, user_id, balance_uid, position_uid, status):
    commissions_fee, stamp_duty_fee, total_fee = calculate_fee(instance.amount, status, user_id)
    
    if commissions_fee:
        fee = OrderFee.objects.create(
            order_uid=instance,
            fee_type=f"{instance.side} commissions fee",
            amount=commissions_fee
        )
    if total_fee:
        balance = Accountbalance.objects.get(user_id=instance.user_id)
        TransactionHistory.objects.create(
            balance_uid=balance_uid,
            side="debit",
            amount=commissions_fee,
            transaction_detail={
                "last_amount" : balance.amount,
                "description": f"{instance.side} fee",
                "position": f"{position_uid}",
                "event": "fee",
                "fee_id": fee.id,
                "order_uid": str(instance.order_uid)
            },
        )
    if stamp_duty_fee:
        stamp = OrderFee.objects.create(
            order_uid=instance,
            fee_type=f"{instance.side} stamp_duty fee",
            amount=stamp_duty_fee
        )
        balance = Accountbalance.objects.get(user_id=instance.user_id)
        TransactionHistory.objects.create(
            balance_uid=balance_uid,
            side="debit",
            amount=stamp_duty_fee,
            transaction_detail={
                "last_amount" : balance.amount,
                "description": f"{instance.side} fee",
                "position": f"{position_uid}",
                "event": "stamp_duty",
                "stamp_duty_id": stamp.id,
                "order_uid": str(instance.order_uid)
            },
        )

def generate_hedge_setup(instance: Order) -> dict:

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

    return setup


@receiver(pre_save, sender=Order)
def order_signal_check(sender, instance, **kwargs):
    
    # this for locking balance before order is filled
    if instance.placed and instance.status == 'placed':
        if instance.status != "filled":
            instance.status = "pending"
    
    # if status not in ["filled", "placed", "pending", "cancel"] and is new order, recalculate price and share
    if not instance.status in ["filled", "placed", "pending", "cancel"] and instance.is_init:
        # if bot will create setup expiry , SL and TP
        if instance.bot_id != "STOCK_stock_0":
            setup = generate_hedge_setup(instance)
            instance.setup = setup
            instance.qty = setup["share_num"]
            instance.amount = formatdigit(setup["share_num"] * setup['price'])
        else:
            instance.setup = None
            instance.qty = math.floor(instance.amount / instance.price)
            instance.amount = round(instance.qty * instance.price,2)


@receiver(post_save, sender=Order)
def order_signal(sender, instance, created, **kwargs):

    print(f"<<<<<<<<<<<< STATUSSSSS CHANGE {instance.status} {instance.order_uid}>>>>>>>>>>>>>>>>>>")
    if created and instance.is_init:
        # if bot will create setup expiry , SL and TP
        # if instance.bot_id != "STOCK_stock_0":
        #     setup = generate_hedge_setup(instance)
        #     instance.setup = setup
        #     instance.qty = setup["share_num"]
        #     instance.amount = formatdigit(setup["share_num"] * setup['price'])
        # else:
        #     instance.setup =None
        #     instance.qty = math.floor(instance.amount / instance.price)
        #     instance.amount = round(instance.qty * instance.price)
        # instance.save()
        pass

        # if not user can create the setup TP and SL
    elif created and not instance.is_init and instance.bot_id != "STOCK_stock_0":
        pass

    elif not created and instance.status in "cancel":
        trans = TransactionHistory.objects.filter(
            side='debit', transaction_detail__description='bot order', transaction_detail__order_uid=str(instance.order_uid))
        if trans.exists():
            trans = trans.get()
            trans.delete()



    elif not created and instance.side == 'buy' and instance.status in ["pending"] and instance.is_init and not PositionPerformance.objects.filter(performance_uid=instance.performance_uid).exists():
        print(f"=================ORDERING {instance.status} {instance.order_uid} ===================")
        # first transaction, user put the money to bot cash balance /in order
        # if the order still in pending state, its cancelable
        # on this state user balance will decrease and lock for orders until it filled / cancels

        if instance.setup and instance.is_init:
            inv_amt = instance.setup['investment_amount']
        else:
            inv_amt = instance.amount
        digits = max(min(5-len(str(int(instance.price))), 2), -1)
        balance = Accountbalance.objects.get(user_id=instance.user_id)
        TransactionHistory.objects.create(
            balance_uid=instance.user_id.wallet,
            side="debit",
            amount=round(inv_amt, digits),
            transaction_detail={
                "last_amount" : balance.amount,
                "description": "bot order",
                # "position": f"{order.position_uid}",
                "event": "create",
                "order_uid": str(instance.order_uid)
            },
        )
        if not instance.order_type:
            commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                instance.amount, instance.side, instance.user_id)
            fee = OrderFee.objects.create(
                order_uid=instance,
                fee_type=f"{instance.side} commissions fee",
                amount=commissions_fee
            )
            balance = Accountbalance.objects.get(user_id=instance.user_id)
            TransactionHistory.objects.create(
                balance_uid=instance.user_id.wallet,
                side="debit",
                amount=total_fee,
                transaction_detail={
                    "description": f"{instance.side} fee",
                    # "position": f"{order.position_uid}",
                    "event": "fee",
                    "fee_id": fee.id
                },
            )
            if stamp_duty_fee > 0:
                stamp = OrderFee.objects.create(
                    order_uid=instance,
                    fee_type=f"{instance.side} stamp_duty fee",
                    amount=stamp_duty_fee
                )
                balance = Accountbalance.objects.get(user_id=instance.user_id)
                TransactionHistory.objects.create(
                    balance_uid=instance.user_id.wallet,
                    side="debit",
                    amount=stamp_duty_fee,
                    transaction_detail={
                        "last_amount" : balance.amount,
                        "description": f"{instance.side} fee",
                        # "position": f"{order.position_uid}",
                        "event": "stamp_duty",
                        "stamp_duty_id": stamp.id
                    },
                )

    elif not created and instance.status in "filled" and not PositionPerformance.objects.filter(performance_uid=instance.performance_uid).exists():
        bot = BotOptionType.objects.get(bot_id=instance.bot_id)

        # update the status and create new positions
        # if order is filled will create the position and first performance
        if instance.is_init:
            print(f"================= INIT FILLED {instance.status} {instance.order_uid} ===================")
            # below is only for new order initiated
            margin = 1
            
            if instance.user_id.is_large_margin and bot.bot_type.bot_type != "CLASSIC":
                margin = 1.5

            if instance.status == "filled":
                spot_date = instance.filled_at
                order = OrderPosition.objects.create(
                    user_id=instance.user_id,
                    ticker=instance.ticker,
                    bot_id=instance.bot_id,
                    spot_date=spot_date.date(),
                    entry_price=instance.price,
                    # investment_amount=instance.amount,
                    is_live=True,
                    margin=margin
                )
                perf = PositionPerformance.objects.create(
                    created=spot_date,
                    position_uid=order,
                    last_spot_price=instance.price,
                    last_live_price=instance.price,
                    order_uid=instance,
                    status='Populate'
                )
            # if use bot
            if instance.setup:

                for key, val in instance.setup.items():
                    if hasattr(perf, key):
                        setattr(perf, key, val)
                    if hasattr(order, key):
                        if key == "share_num":
                            continue
                        else:
                            setattr(order, key, val)
                    else:
                        if key == "total_bot_share_num":
                            setattr(order, "share_num", val)
            else:
                # for user without bot
                order.investment_amount = instance.amount
                order.bot_cash_balance = 0
                order.share_num = instance.qty
                perf.share_num = instance.qty
            # start creating position
            digits = max(min(5-len(str(int(perf.last_live_price))), 2), -1)
            perf.current_pnl_amt = 0  # need to calculate with fee
            perf.current_bot_cash_balance = order.bot_cash_balance

            perf.current_investment_amount = round(
                perf.last_live_price * perf.share_num, digits)
            perf.current_pnl_ret = (perf.current_bot_cash_balance + perf.current_investment_amount -
                                    order.investment_amount) / order.investment_amount
            perf.order_id = instance
            perf.save()
            order.save()
            instance.performance_uid = perf.performance_uid
               
            instance.save()

            update_initial_transaction_position(instance,order.position_uid)
    
    # elif not created and instance.status in "filled" and performance_exist:
        else:
            # hedging daily bot here
            if not bot.is_stock():
                print(f"================= HEDGE FILLED {instance.status} {instance.order_uid} ===================")
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

                    # should be no transaction here except fee coz user already  put the money into bot
                    if instance.side == "sell" and order_position.is_live and not instance.order_type:
                        print(f"================= Hedge Sell ORDER success {instance.status} {instance.order_uid} ===================")

                        commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                            instance.amount, "sell", order_position.user_id)

                        fee = OrderFee.objects.create(
                            order_uid=instance,
                            fee_type=f"{instance.side} commissions fee",
                            amount=commissions_fee
                        )
                        balance = Accountbalance.objects.get(user_id=instance.user_id)
                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="debit",
                            amount=total_fee,
                            transaction_detail={
                                "last_amount" : balance.amount,
                                "description": f"{instance.side} fee",
                                "position": f"{order_position.position_uid}",
                                "event": "fee",
                                "fee_id": fee.id
                            },
                        )
                        if stamp_duty_fee > 0:
                            stamp = OrderFee.objects.create(
                                order_uid=instance,
                                fee_type=f"{instance.side} stamp_duty fee",
                                amount=stamp_duty_fee
                            )
                            balance = Accountbalance.objects.get(user_id=instance.user_id)

                            TransactionHistory.objects.create(
                                balance_uid=order_position.user_id.wallet,
                                side="debit",
                                amount=stamp_duty_fee,
                                transaction_detail={
                                    "last_amount" : balance.amount,
                                    "description": f"{instance.side} fee",
                                    "position": f"{order_position.position_uid}",
                                    "event": "stamp_duty",
                                    "stamp_duty_id": stamp.id
                                },
                            )
                    elif instance.side == "buy" and order_position.is_live and not instance.order_type:
                        print(f"================= Hedge Buy ORDER success {instance.status} {instance.order_uid} ===================")

                        commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                            instance.amount, "buy", order_position.user_id)
                        fee = OrderFee.objects.create(
                            order_uid=instance,
                            fee_type=f"{instance.side} commissions fee",
                            amount=total_fee
                        )
                        balance = Accountbalance.objects.get(user_id=instance.user_id)

                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="debit",
                            amount=total_fee,
                            transaction_detail={
                                "last_amount" : balance.amount,
                                "description": f"{instance.side} fee",
                                "position": f"{order_position.position_uid}",
                                "event": "fee",
                                "fee_id": fee.id
                            },
                        )
                        if stamp_duty_fee > 0:
                            stamp = OrderFee.objects.create(
                                order_uid=instance,
                                fee_type=f"{instance.side} stamp_duty fee",
                                amount=stamp_duty_fee
                            )
                            balance = Accountbalance.objects.get(user_id=instance.user_id)

                            TransactionHistory.objects.create(
                                balance_uid=order_position.user_id.wallet,
                                side="debit",
                                amount=stamp_duty_fee,
                                transaction_detail={
                                    "last_amount" : balance.amount,
                                    "description": f"{instance.side} fee",
                                    "position": f"{order_position.position_uid}",
                                    "event": "stamp_duty",
                                    "stamp_duty_id": stamp.id
                                },
                            )

                    # end portfolio / bots
                    if not order_position.is_live:
                        print(f"================= Hedge Stop/finish ORDER success {instance.status} {instance.order_uid} ===================")

                        # add bot_cash_dividend on return
                        amt = order_position.investment_amount + order_position.final_pnl_amount
                        return_amt = amt + order_position.bot_cash_dividend
                        balance = Accountbalance.objects.get(user_id=instance.user_id)
                        TransactionHistory.objects.create(
                            balance_uid=order_position.user_id.wallet,
                            side="credit",
                            amount=return_amt,
                            transaction_detail={
                                "last_amount" : balance.amount,
                                "description": "bot return",
                                "position": f"{order_position.position_uid}",
                                "event": "return",
                                "order_uid": str(instance.order_uid)
                            },
                        )
                        if not instance.order_type:
                            commissions_fee, stamp_duty_fee, total_fee = calculate_fee(
                                amt, "sell", order_position.user_id)
                            fee = OrderFee.objects.create(
                                order_uid=instance,
                                fee_type=f"{instance.side} commissions fee",
                                amount=commissions_fee
                            )
                            balance = Accountbalance.objects.get(user_id=instance.user_id)

                            TransactionHistory.objects.create(
                                balance_uid=order_position.user_id.wallet,
                                side="debit",
                                amount=total_fee,
                                transaction_detail={
                                    "last_amount" : balance.amount,
                                    "description": f"{instance.side} fee",
                                    "position": f"{order_position.position_uid}",
                                    "event": "fee",
                                    "fee_id": fee.id
                                },
                            )
                            if stamp_duty_fee > 0:
                                stamp = OrderFee.objects.create(
                                    order_uid=instance,
                                    fee_type=f"{instance.side} stamp_duty fee",
                                    amount=stamp_duty_fee
                                )
                                balance = Accountbalance.objects.get(user_id=instance.user_id)
                                TransactionHistory.objects.create(
                                    balance_uid=order_position.user_id.wallet,
                                    side="debit",
                                    amount=stamp_duty_fee,
                                    transaction_detail={
                                        "last_amount" : balance.amount,
                                        "description": f"{instance.side} fee",
                                        "position": f"{order_position.position_uid}",
                                        "event": "stamp_duty",
                                        "stamp_duty_id": stamp.id

                                    },
                                )


@receiver(post_delete, sender=PositionPerformance)
def order_revert(sender, instance, **kwargs):
    if instance.order_uid:
        order = Order.objects.get(order_uid=instance.order_uid.order_uid)
        in_wallet_transactions = TransactionHistory.objects.filter(
            transaction_detail__order_uid=str(order.order_uid))
        if in_wallet_transactions.exists():
            in_wallet_transactions.get().delete()
        position = OrderPosition.objects.get(
            position_uid=instance.position_uid.position_uid)

        # return to bot cash balance
        if order.side == 'sell':
            position.bot_cash_balance = position.bot_cash_balance - order.amount
        elif order.side == 'buy':
            position.bot_cash_balance = position.bot_cash_balance + order.amount
        order.delete()
        if position.order_position.all().exists():
            if not position.is_live:
                position.is_live = True
            position.save()
        else:
            position.delete()


@receiver(post_delete, sender=OrderFee)
def return_fee_to_wallet(sender, instance, **kwargs):
    in_wallet_transactions_fee = TransactionHistory.objects.filter(
        transaction_detail__fee_id=instance.id)
    in_wallet_transactions_stamp = TransactionHistory.objects.filter(
        transaction_detail__stamp_duty_id=instance.id)
    if in_wallet_transactions_fee.exists():
        in_wallet_transactions_fee.get().delete()
    if in_wallet_transactions_stamp.exists():
        in_wallet_transactions_stamp.get().delete()
