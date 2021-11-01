from random import choice

import pytest
from bot.calculate_bot import check_date, get_expiry_date
from core.bot.models import BotOptionType

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_api_create_order_with_classic_bot(
    authentication,
    client,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers).values()

    data = {
        "ticker": ticker,
        "price": price,
        "bot_id": "CLASSIC_classic_003846",
        "amount": 10000,
        "user": user.id,
        "side": "buy",
    }

    response = client.post(
        path="/api/order/create/",
        data=data,
        **authentication,
    )

    if (
        response.status_code != 201
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    order = response.json()

    assert order is not None
    assert order["order_uid"] is not None
    assert order["qty"] == order["setup"]["performance"]["share_num"]
    # confirm if the setup is not empty
    assert order["setup"] is not None

    # confirm if the expiry is set correctly
    bot = BotOptionType.objects.get(bot_id="CLASSIC_classic_003846")

    expiry = get_expiry_date(
        bot.time_to_exp,
        order["created"],
        "HKD",
        apps=True,
    )
    expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")
    assert order["setup"]["position"]["expiry"] == expiry_date


def test_api_create_order_with_uno_bot(
    authentication,
    client,
    user,
    tickers,
) -> None:
    ticker, price = choice(tickers).values()

    data = {
        "ticker": ticker,
        "price": price,
        "bot_id": "UNO_ITM_003846",
        "amount": 10000,
        "user": user.id,
        "side": "buy",
        "margin": 2,
    }

    response = client.post(
        path="/api/order/create/",
        data=data,
        **authentication,
    )

    if (
        response.status_code != 201
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    order = response.json()

    assert order is not None
    assert order["order_uid"] is not None
    # confirm if the qty is correctly counted with margin
    assert order["qty"] == order["setup"]["performance"]["share_num"]
    # confirm if the setup is not empty
    assert order["setup"] is not None

    # confirm if the expiry is set correctly
    bot = BotOptionType.objects.get(bot_id="UNO_ITM_003846")

    expiry = get_expiry_date(
        bot.time_to_exp,
        order["created"],
        "HKD",
        apps=True,
    )
    expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")
    assert order["setup"]["position"]["expiry"] == expiry_date


def test_api_create_order_with_ucdc_bot(authentication, client, user, tickers) -> None:
    ticker, price = choice(tickers).values()

    data = {
        "ticker": ticker,
        "price": price,
        "bot_id": "UCDC_ATM_003846",
        "amount": 10000,
        "user": user.id,
        "side": "buy",
        "margin": 2,
    }

    response = client.post(
        path="/api/order/create/",
        data=data,
        **authentication,
    )

    if (
        response.status_code != 201
        or response.headers["Content-Type"] != "application/json"
    ):
        assert False

    order = response.json()

    assert order is not None
    assert order["order_uid"] is not None
    # confirm if the qty is correctly counted with margin
    assert order["qty"] == order["setup"]["performance"]["share_num"]
    # confirm if the setup is not empty
    assert order["setup"] is not None

    # confirm if the expiry is set correctly
    bot = BotOptionType.objects.get(bot_id="UCDC_ATM_003846")

    expiry = get_expiry_date(
        bot.time_to_exp,
        order["created"],
        "HKD",
        apps=True,
    )
    expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")
    assert order["setup"]["position"]["expiry"] == expiry_date
