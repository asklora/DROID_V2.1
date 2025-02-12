import math
import random

import pytest
from core.universe.models import Currency
from core.user.convert import ConvertMoney

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

        if decimal:
            result: float = math.floor(amount * rate * 100) / 100
        else:
            result: float = math.floor(amount * rate)

        return result

    # we get all currency data
    currencies = list(Currency.objects.filter(is_active=True))

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
