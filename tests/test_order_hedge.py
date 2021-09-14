from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from core.master.models import MasterOhlcvtr
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import Accountbalance, TransactionHistory
from portfolio import classic_position_check, ucdc_position_check, uno_position_check

from utils import create_buy_order

pytestmark = pytest.mark.django_db


def test_should_create_hedge_order_for_classic_bot(user) -> None:
    # step 1: create a new order
    ticker = "6606.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-06-01",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="CLASSIC_classic_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # step 2: setup hedge
    classic_position_check(
        position_uid=position.position_uid,
        tac=True,
    )
    # step 3: get hedge positions
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid)

    print(performance.count())

    assert performance.exists() == True
    assert performance.count() > 1


def test_should_create_hedge_order_for_uno_bot(user) -> None:
    # step 1: create a new order
    ticker = "3690.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-06-01",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # step 2: setup hedge
    uno_position_check(
        position_uid=position.position_uid,
        tac=True,
    )
    # step 3: get hedge positions
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid)

    print(performance.count())

    assert performance.exists() == True
    assert performance.count() > 1


def test_should_create_hedge_order_for_ucdc_bot(user) -> None:
    # step 1: create a new order
    ticker = "2282.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-06-01",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # step 2: setup hedge
    ucdc_position_check(
        position_uid=position.position_uid,
        tac=True,
    )

    # step 3: get hedge positions
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid)

    print(performance.count())

    assert performance.exists() == True
    assert performance.count() > 1


def test_should_create_hedge_order_for_ucdc_bot_with_margin(user) -> None:
    # step 1: create a new order
    ticker = "2282.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-06-01",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        margin=2,
        bot_id="UCDC_ATM_007692",
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # step 2: setup hedge
    ucdc_position_check(
        position_uid=position.position_uid,
        tac=True,
    )

    # step 3: get hedge positions
    performance = PositionPerformance.objects.filter(position_uid=position.position_uid)

    print(performance.count())

    assert performance.exists() == True
    assert performance.count() > 1


def test_hedge_values_for_ucdc_bot(user) -> None:
    # step 1: create a new order
    ticker = "2020.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-05-03",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        margin=2,
        bot_id="UCDC_ATM_007692",  # 4 weeks worth of testing
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # step 2: setup hedge
    ucdc_position_check(
        position_uid=position.position_uid,
        tac=True,
    )

    # step 3: get hedge values
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

    orders = Order.objects.filter(order_uid__in=performance_df["order_uid"].to_list())
    orders_df = pd.DataFrame(list(orders.values()))
    orders_df = orders_df[["order_uid", "order_type", "side", "amount", "price", "qty"]]

    print(position_df)
    print(position_df.columns)
    print(position.count())

    print(performance_df)
    print(performance_df.columns)
    print(performance.count())

    print(orders_df)
    print(orders_df.columns)
    print(orders.count())

    performance_df = performance_df.sort_values(by=["created"])
    performance_df = performance_df.merge(position_df, how="left", on=["position_uid"])
    performance_df = performance_df.merge(orders_df, how="left", on=["order_uid"])

    # performance_df = pd.read_csv("hedge_margin.csv")
    print(performance_df)
    print(performance_df.columns)
    performance_df["real_share_num"] = performance_df["share_num"].shift() + (
        (
            performance_df["last_hedge_delta"]
            - performance_df["last_hedge_delta"].shift()
        )
        * performance_df["bot_share_num"]
    ).round(0)
    performance_df["real_investment_amount"] = (
        performance_df["real_share_num"] * performance_df["last_live_price"]
    )
    performance_df["real_pnl_amt"] = (
        performance_df["current_pnl_amt"].shift()
        + (
            performance_df["last_live_price"]
            - performance_df["last_live_price"].shift()
        )
        * performance_df["share_num"].shift()
    )
    performance_df["real_pnl_ret"] = (
        performance_df["real_pnl_amt"] / performance_df["investment_amount"]
    ).round(4)
    performance_df["real_qty"] = (
        performance_df["real_share_num"] - performance_df["share_num"].shift()
    )

    performance_df["real_side"] = ""
    performance_df["real_side"] = np.where(
        performance_df["real_qty"] > 0, "buy", performance_df["real_side"]
    )
    performance_df["real_side"] = np.where(
        performance_df["real_qty"] < 0, "sell", performance_df["real_side"]
    )

    performance_df["margin_amount"] = (1 - performance_df["margin"]) * performance_df[
        "investment_amount"
    ]
    performance_df["margin_threshold"] = (
        performance_df["margin_amount"] - performance_df["current_bot_cash_balance"]
    )
    performance_df["status_margin"] = np.where(
        performance_df["margin_threshold"] <= 0, True, False
    )
    performance_df["status_share_num"] = np.where(
        performance_df["real_share_num"] == performance_df["share_num"], True, False
    )
    performance_df["status_investment_amount"] = np.where(
        performance_df["real_investment_amount"] == performance_df["investment_amount"],
        True,
        False,
    )
    performance_df["status_side"] = np.where(
        performance_df["real_side"] == performance_df["side"], True, False
    )
    performance_df["status_qty"] = np.where(
        performance_df["real_qty"] == performance_df["qty"], True, False
    )
    performance_df["status_pnl"] = np.where(
        performance_df["real_pnl_amt"] == performance_df["current_pnl_amt"], True, False
    )
    performance_df["status_pnl_ret"] = np.where(
        performance_df["real_pnl_ret"] == performance_df["current_pnl_ret"], True, False
    )
    performance_df.to_csv("hedge_margin.csv")

    # Syarat Lulus Test
    status = True
    status = (
        status
        and len(performance_df.loc[performance_df["status_margin"] == False]) == 0
    )
    status = (
        status
        and len(performance_df.loc[performance_df["status_share_num"] == False]) == 0
    )
    status = (
        status
        and len(performance_df.loc[performance_df["status_investment_amount"] == False])
        == 0
    )
    status = (
        status and len(performance_df.loc[performance_df["status_side"] == False]) == 0
    )
    status = (
        status and len(performance_df.loc[performance_df["status_qty"] == False]) == 0
    )
    status = (
        status and len(performance_df.loc[performance_df["status_pnl"] == False]) == 0
    )
    status = (
        status
        and len(performance_df.loc[performance_df["status_pnl_ret"] == False]) == 0
    )
    status = (
        status and len(performance_df.loc[performance_df["status"] == "Populate"]) == 1
    )

    assert performance.exists() == True
    assert performance.count() > 1


def test_bot_and_user_balance_movements_for_ucdc_bot(user) -> None:
    # step 1: create a new order
    ticker = "9901.HK"
    master = MasterOhlcvtr.objects.get(
        ticker=ticker,
        trading_day="2021-06-01",
    )
    price = master.close
    log_time = datetime.combine(master.trading_day, datetime.min.time())

    buy_order = create_buy_order(
        ticker=ticker,
        price=price,
        user_id=user.id,
        margin=2,
        bot_id="UCDC_ATM_003846",  # 2 weeks worth of testing
    )

    buy_order.status = "placed"
    buy_order.placed = True
    buy_order.placed_at = log_time
    buy_order.save()

    buy_order.status = "filled"
    buy_order.filled_at = log_time
    buy_order.save()

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(pk=performance.position_uid_id)

    # step 2: setup hedge
    ucdc_position_check(
        position_uid=position.position_uid,
        tac=True,
    )

    # get hedge performances
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
            "final_pnl_amount",
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

    # get user balances
    balance = Accountbalance.objects.get(user=user)

    # get user transaction history
    transactions = TransactionHistory.objects.filter(balance_uid=balance)
    transactions_df = pd.DataFrame(list(transactions.values()))
    transactions_df = transactions_df.sort_values("created", ascending=False)
    transactions_df = transactions_df[
        ["id", "created", "side", "amount", "transaction_detail"]
    ]

    # get transaction details
    transaction_details_df = pd.json_normalize(transactions_df["transaction_detail"])
    transaction_details_df = transaction_details_df[
        ["event", "order_uid", "last_amount"]
    ]
    transactions_df = transactions_df.rename(
        {
            "id": "transaction_id",
            "amount": "transaction_amount",
            "balance_uid_id": "balance_uid",
        },
        axis=1,
    )

    # join both transaction dataframes
    transactions_df = transactions_df.join(transaction_details_df)

    # calculate user wallet amounts
    transactions_df["wallet_amount"] = np.where(
        transactions_df["side"] == "credit",
        transaction_details_df["last_amount"].fillna(0)
        + transactions_df["transaction_amount"],
        transaction_details_df["last_amount"].fillna(0)
        - transactions_df["transaction_amount"],
    )

    # we only take the columns we need
    # transactions_df = transactions_df[
    #     ["order_uid", "transaction_amount", "wallet_amount"]
    # ]

    # we join all interesting dataframes
    performance_df = performance_df.sort_values(by=["created"])
    performance_df = performance_df.merge(position_df, how="left", on=["position_uid"])
    performance_df = performance_df.merge(orders_df, how="left", on=["order_uid"])

    transactions_df.to_csv("user_transactions.csv")
    # performance_df.to_csv("hedge_user_balance.csv")
