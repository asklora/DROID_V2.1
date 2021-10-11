from datetime import datetime

import pytest
from core.djangomodule.general import formatdigit, jsonprint
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.services import sell_position_service
from core.user.convert import ConvertMoney
from core.user.models import Accountbalance, User
from tests.utils.order import create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_sell_order_with_conversion(user):
    ticker: str = "XOM"
    price: float = 60.93
    amount: float = 10000

    # converter to usd
    usd_conversion = ConvertMoney(user.currency, "USD")
    usd_conversion_result = usd_conversion.convert(amount)

    print(f"conversion result: {usd_conversion_result}")

    wallet = Accountbalance.objects.get(user=user)
    user_balance = formatdigit(wallet.amount, wallet.currency_code.is_decimal)

    order: Order = create_buy_order(
        price=price,
        ticker=ticker,
        amount=amount,
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    order.status = "placed"
    order.placed = True
    order.placed_at = datetime.now()
    order.save()

    order.status = "filled"
    order.filled_at = datetime.now()
    order.save()

    # check if user balance is deducted
    wallet = Accountbalance.objects.get(user=user)
    user_balance_2 = formatdigit(wallet.amount, False)
    print(f"buy order amount: {order.amount}")
    print(f"user balance after order: {user_balance_2}")
    assert user_balance != user_balance_2
    assert (
        formatdigit(user_balance - amount, wallet.currency_code.is_decimal)
        == user_balance_2
    )

    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid=order.order_uid,
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    print(f"position bot balance: {position.bot_cash_balance}")
    print(f"position amount: {position.investment_amount}")

    # We create the sell order
    sell_position, sell_order = sell_position_service(
        price - 2.0,
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert confirmed_sell_order

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    confirmed_sell_order.status = "filled"
    confirmed_sell_order.filled_at = datetime.now()
    confirmed_sell_order.save()

    sell_position = OrderPosition.objects.get(position_uid=sell_position.position_uid)
    assert not sell_position.is_live

    jsonprint(confirmed_sell_order.setup)

    assert usd_conversion_result == sell_position.investment_amount

    print(f"final pnl amount: {sell_position.final_pnl_amount}")

    assert (
        abs(
            confirmed_sell_order.setup["performance"]["last_live_price"]
            * confirmed_sell_order.setup["performance"]["order_summary"]["hedge_shares"]
        )
    ) == confirmed_sell_order.amount

    hkd_conversion = ConvertMoney("USD", user.currency)
    hkd_conversion_result = hkd_conversion.convert(
        sell_position.final_pnl_amount + sell_position.investment_amount
    )
    print(hkd_conversion_result)

    wallet = Accountbalance.objects.get(user=user)
    user_balance_3 = formatdigit(wallet.amount, False)

    assert formatdigit(user_balance_2 + hkd_conversion_result, False) == user_balance_3
