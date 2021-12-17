from random import choice

import pytest
from tests.utils.order import create_buy_order

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_create_new_buy_order_for_classic_bot(
    user,
    tickers,
) -> None:
    """
    A new BUY order should be created with non-empty setup
    """

    bot_id = "CLASSIC_classic_007692"
    ticker, price = choice(tickers)

    order = create_buy_order(
        bot_id=bot_id,
        ticker=ticker,
        price=price,
        user_id=user.id,
    )

    print(order.setup)

    assert order.side == "buy"
    # Setup should be populated with bot information
    assert order.setup is not None
    assert order.bot_id == bot_id


def test_create_new_buy_order_for_uno_bot(
    user,
    tickers,
) -> None:
    """
    A new BUY order should be created with non-empty setup
    """

    bot_id = "UNO_OTM_007692"
    ticker, price = choice(tickers)

    order = create_buy_order(
        bot_id=bot_id,
        ticker=ticker,
        price=price,
        user_id=user.id,
    )

    assert order.side == "buy"
    # Setup should be populated with bot information
    assert order.setup is not None
    assert order.bot_id == bot_id


def test_create_new_buy_order_for_ucdc_bot(
    user,
    tickers,
) -> None:
    """
    A new BUY order should be created with non-empty setup
    """

    bot_id = "UCDC_ATM_007692"
    ticker, price = choice(tickers)

    order = create_buy_order(
        bot_id=bot_id,
        ticker=ticker,
        price=price,
        user_id=user.id,
    )

    assert order.side == "buy"
    # Setup should be populated with bot information
    assert order.setup is not None
    assert order.bot_id == bot_id
