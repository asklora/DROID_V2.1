import random
from datetime import datetime
from typing import List

import pytest
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.orders.services import sell_position_service
from core.universe.models import Currency, Universe
from core.user.convert import ConvertMoney

from tests.utils import create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_currency_conversion():
    # our own conversion function
    def convert(
        from_currency: Currency,
        to_currency: Currency,
        amount: float,
        decimal: bool = True,
    ) -> float:
        rate: float = (
            1.0
            if from_currency.last_price == to_currency.last_price
            else to_currency.last_price / from_currency.last_price
        )
        rounding: int = 2 if decimal else 0

        result: float = amount * rate
        return round(result, rounding)

    # we get all currency data
    currencies = Currency.objects.filter(is_active=True)

    # we do the assertion three times
    for _ in range(3):
        # setting this as the amount to convert
        amount: float = round(random.uniform(5.0, 1000.0), 2)

        # we select 2 random currencies
        currency_1: Currency = random.choice(currencies)
        currency_2: Currency = random.choice(currencies)

        # the one we are testing
        conversion = ConvertMoney(currency_1, currency_2)
        conversion_result = conversion.convert(amount)

        # our own function
        test_conversion_result = convert(
            from_currency=currency_1,
            to_currency=currency_2,
            amount=amount,
            decimal=currency_2.is_decimal,
        )

        print(f"\nconverting {amount} {currency_1} into {currency_2}")
        print(f"ConvertMoney result: {conversion_result} {currency_2}")
        print(f"Our function result: {test_conversion_result} {currency_2}")

        assert conversion_result == test_conversion_result


def test_buy_order_with_conversion(user):
    # We get the tickers
    usd_tickers: List[Universe] = Universe.objects.filter(
        currency_code="USD",
        is_active=True,
    ).values_list("ticker", flat=True)

    # We turn them into list of tickers
    tickers: List[str] = [str(elem) for elem in list(usd_tickers)]

    ticker: str = random.choice(tickers)
    price: float = 60.93
    amount: float = 10000

    # the one we are testing
    conversion = ConvertMoney(user.currency, "USD")
    conversion_result = conversion.convert(amount)

    order: Order = create_buy_order(
        price=price,
        ticker=ticker,
        amount=conversion_result,
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

    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid_id=order.order_uid
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    print(f"Conversion result: {conversion_result}")
    print(f"Position investment amt: {position.investment_amount}")

    assert conversion_result == position.investment_amount


def test_sell_order_with_conversion(user):
    ticker: str = "XOM"
    price: float = 60.93
    amount: float = 10000

    # converter to usd
    usd_conversion = ConvertMoney(user.currency, "USD")
    usd_conversion_result = usd_conversion.convert(amount)

    order: Order = create_buy_order(
        price=price,
        ticker=ticker,
        amount=usd_conversion_result,
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

    performance: PositionPerformance = PositionPerformance.objects.get(
        order_uid=order.order_uid,
    )

    position: OrderPosition = OrderPosition.objects.get(
        pk=performance.position_uid_id,
    )

    # We create the sell order
    sellPosition, sell_order = sell_position_service(
        price,
        datetime.now(),
        position.position_uid,
    )

    confirmed_sell_order = Order.objects.get(pk=sell_order.pk)
    assert confirmed_sell_order

    # the one we are testing
    hkd_conversion = ConvertMoney(confirmed_sell_order.amount, "HKD")
    hkd_conversion_result = hkd_conversion.convert(amount)

    confirmed_sell_order.status = "placed"
    confirmed_sell_order.placed = True
    confirmed_sell_order.placed_at = datetime.now()
    confirmed_sell_order.save()

    print(confirmed_sell_order.setup)

    assert (
        confirmed_sell_order.setup["position"]["bot_cash_balance"]
        == hkd_conversion_result
    )
