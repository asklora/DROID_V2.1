from datetime import datetime

import pytest
from core.master.models import MasterOhlcvtr
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import Accountbalance, TransactionHistory
from django.db.models.query import QuerySet
from portfolio import ucdc_position_check
from tests.utils.order import confirm_order, create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


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
        created=log_time,
        ticker=ticker,
        price=price,
        user_id=user.id,
        bot_id="UCDC_ATM_003846",  # 2 weeks worth of testing
    )

    confirm_order(buy_order, log_time)

    confirmed_buy_order = Order.objects.get(pk=buy_order.pk)

    performance = PositionPerformance.objects.get(
        order_uid_id=confirmed_buy_order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    print(f"expiry: {position.expiry}")

    # step 2: setup hedge
    ucdc_position_check(
        position_uid=position.position_uid,
        tac=True,
    )

    # get hedge positions
    positions = OrderPosition.objects.filter(
        position_uid=position.position_uid,
    )

    # get hedge performances
    performances = PositionPerformance.objects.filter(
        position_uid=position.position_uid
    )

    # get user balances
    balance = Accountbalance.objects.get(user=user)

    # get user transaction history
    transactions: QuerySet = TransactionHistory.objects.filter(
        balance_uid=balance,
    )

    # we check if the hedge created performances data
    assert performances.exists()
    assert len(performances) > 1

    # we check if the user get the investment monye back from bot
    # first transaction is the initial deposit,
    # and the last one is the bot return
    assert len(transactions) >= 3
    assert transactions.last().transaction_detail["description"] == "bot return"

    print(f"\ninvestment amount: {positions.last().investment_amount}")
    print(f"final pnl amount: {positions.last().final_pnl_amount}")
    print(f"bot return amount: {transactions.last().amount}")

    # we see if the bot returns the correct amount of money
    assert (
        positions.last().investment_amount + positions.last().final_pnl_amount
        == transactions.last().amount
    )
