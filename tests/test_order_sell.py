from datetime import datetime
import json
import pytest
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.services import sell_position_service
from core.user.models import Accountbalance, TransactionHistory, User

import numpy as np
import pandas as pd
from utils import create_buy_order

pytestmark = pytest.mark.django_db


def test_should_create_new_sell_order_for_user(user) -> None:
    """
    A new SELL order should be created from a buy order
    """

    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
    )

    # We set it as filled
    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    # We get the order to update the data
    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    # Check if it successfully is added to the performance table
    assert confirmed_buy_order.performance_uid != None

    # We get the position and performance data to update it
    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    # We confirm the above test
    assert performance != None

    # We then get the position based on the performance data
    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # We confirm if the position is set
    assert position != None

    # We create the sell order
    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid != None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    # We confirm that the selling is successfully finished by checking the user balance
    user_balance = Accountbalance.objects.get(user=user).amount
    assert user_balance != previous_user_balance


def test_should_create_new_sell_order_for_user_with_classic_bot(user) -> None:
    price = 6.11

    # We create an order
    buy_order = create_buy_order(
        ticker="1199.HK",
        price=price,
        user_id=user.id,
        bot_id="CLASSIC_classic_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        price + 3.0,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid != None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()


def test_should_create_new_sell_order_for_user_with_uno_bot(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid != None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()


def test_should_create_new_sell_order_for_user_with_ucdc_bot(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid != None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()


def test_asasasasasasa(user) -> None:
    price = 1317

    # We create an order
    buy_order = create_buy_order(
        ticker="6606.HK",
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = datetime.now()
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = datetime.now()
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    sellPosition, sell_order = sell_position_service(
        price + 13,  # Selling in different price point (1317 + 13 = 1330 here)
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert sell_order.order_uid != None

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    # We get previous user balance
    user = User.objects.get(pk=user.id)
    previous_user_balance = Accountbalance.objects.get(user=user).amount

    # We accept the order and set it as filled
    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    # get performance
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid)
    performance_df = pd.DataFrame(list(performance.values()))
    performance_df = performance_df[
        [
            "created",
            "position_uid_id",
            "performance_uid",
            "last_spot_price",
            "last_live_price",
            "current_pnl_ret",
            "current_pnl_amt",
            "current_bot_cash_balance",
            "share_num",
            "current_investment_amount",
            "last_hedge_delta",
            "order_summary",
            "order_uid_id",
            "status",
        ]
    ]

    performance_df = performance_df.rename(
        columns={"position_uid_id": "position_uid", "order_uid_id": "order_uid"}
    )

    # get hedge positions
    position = OrderPosition.objects.filter(position_uid=position.position_uid)
    position_df = pd.DataFrame(list(position.values()))
    position_df = position_df[
        [
            "position_uid",
            "user_id_id",
            "ticker_id",
            "bot_id",
            "expiry",
            "spot_date",
            "entry_price",
            "investment_amount",
            "bot_cash_balance",
            "share_num",
            "margin",
            "bot_cash_dividend",
        ]
    ]
    position_df = position_df.rename(
        columns={
            "user_id_id": "user_id",
            "ticker_id": "ticker",
            "share_num": "bot_share_num",
        }
    )

    # get hedge orders
    orders = Order.objects.filter(order_uid__in=performance_df["order_uid"].to_list())
    orders_df = pd.DataFrame(list(orders.values()))
    orders_df = orders_df[["order_uid", "order_type", "side", "amount", "price", "qty"]]
    orders = orders_df.rename(columns={"amount": "order_amount"})

    # get user balances
    balance = Accountbalance.objects.get(user=user)

    # get user transaction history
    transactions = TransactionHistory.objects.filter(balance_uid=balance)
    transactions_df = pd.DataFrame(list(transactions.values()))
    transactions_df = transactions_df[["id", "side", "amount", "balance_uid_id", "transaction_detail"]]
    transactions_df = transactions_df.join(pd.json_normalize(transactions_df['transaction_detail']))
    transactions_df = transactions_df.rename(
        {
            "id": "transaction_id",
            "amount": "transaction_amount",
            "balance_uid_id": "balance_uid",
            "position": "position_uid",
        }, axis=1,
    )
    
    print(performance_df.count())
    print(transactions_df.count())
    print(transactions_df.count())

    performance_df = performance_df.sort_values(by=["created"])
    performance_df = performance_df.merge(position_df, how="left", on=["position_uid"])
    performance_df = performance_df.merge(orders_df, how="left", on=["order_uid"])
    performance_df = performance_df.merge(transactions_df, how="left", on=["position_uid"])

    performance_df.to_csv("anu.csv")
