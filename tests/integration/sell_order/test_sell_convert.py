import math

import pytest
from core.djangomodule.general import jsonprint
from core.orders.factory.orderfactory import OrderController, SellOrderProcessor
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.convert import ConvertMoney
from core.user.models import Accountbalance
from tests.utils.order import (
    confirm_order,
    create_buy_order,
    get_position_performance,
)

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_sell_order_with_conversion(user):
    ticker: str = "EA.O"
    price: float = 128.41
    amount: float = 10000

    # converter to usd
    usd_conversion = ConvertMoney(user.currency, "USD")
    usd_conversion_result = usd_conversion.convert(amount)

    print(f"conversion result: {usd_conversion_result}")

    wallet = Accountbalance.objects.get(user=user)
    user_balance = math.floor(wallet.amount)

    order: Order = create_buy_order(
        price=price,
        ticker=ticker,
        amount=amount,
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    confirm_order(order)

    # check if user balance is deducted
    wallet = Accountbalance.objects.get(user=user)
    user_balance_2 = math.floor(wallet.amount)
    print(f"buy order amount: {order.amount}")
    print(f"user balance after order: {wallet.amount}")
    assert user_balance != user_balance_2
    assert math.floor(user_balance - amount) == user_balance_2

    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid=order.order_uid,
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    print(f"position bot balance: {position.bot_cash_balance}")
    print(f"position amount: {position.investment_amount}")

    # We create the sell order
    buy_position, _ = get_position_performance(order)

    order_payload: dict = {
        "setup": {"position": buy_position.position_uid},
        "side": "sell",
        "ticker": order.ticker,
        "user_id": order.user_id,
        "margin": order.margin,
    }

    controller: OrderController = OrderController()

    sell_order: Order = controller.process(
        SellOrderProcessor(order_payload),
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert confirmed_sell_order

    confirm_order(confirmed_sell_order)

    sell_position, _ = get_position_performance(sell_order)
    assert not sell_position.is_live

    jsonprint(confirmed_sell_order.setup)

    assert usd_conversion_result == sell_position.investment_amount

    print(f"final pnl amount: {sell_position.final_pnl_amount}")

    assert round(
        abs(
            confirmed_sell_order.setup["performance"]["last_live_price"]
            * confirmed_sell_order.setup["performance"]["order_summary"]["hedge_shares"]
        )
    ) == round(confirmed_sell_order.amount)

    hkd_conversion = ConvertMoney("USD", user.currency)
    hkd_conversion_result = hkd_conversion.convert(
        sell_position.final_pnl_amount + sell_position.investment_amount
    )
    print(hkd_conversion_result)

    wallet = Accountbalance.objects.get(user=user)
    user_balance_3 = math.floor(wallet.amount)

    # assert math.ceil(user_balance_2 + hkd_conversion_result) == user_balance_3
