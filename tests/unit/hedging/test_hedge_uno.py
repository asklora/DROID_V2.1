import random
from datetime import datetime
from typing import List

import pytest
from bot.factory.act.creator import UnoCreator
from bot.factory.BaseFactory import BotFactory
from bot.factory.validator import BotCreateProps
from core.master.models import LatestPrice
from core.djangomodule.general import jsonprint

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_create_bot() -> None:
    # first we initialize the bot properties
    props = BotCreateProps(
        ticker="1211.HK",
        spot_date=datetime.now(),
        investment_amount=100000,
        price=290.1,
        bot_id="UNO_ITM_015384",
    )

    # we start the factory
    factory = BotFactory()

    # we find the correct creator class
    # based on the bot type using the props above
    creator = factory.get_creator(props)

    # we init the creator
    res = creator.create()

    # get the result
    result: dict = res.get_result_as_dict()
    print(result)

    # check if the result matches the original
    # props we set above
    assert result.get("ticker") == props.ticker
    assert result.get("bot_id") == props.get_bot()

    # we check the values
    assert result.get("t") is not None
    assert result.get("r") is not None
    assert result.get("q") is not None

    assert result.get("margin") == 1
    assert result.get("delta") == -0.1414841499677387
    assert result.get("strike") == 320.03549531136446
    assert result.get("barrier") == 409.84198124545765

    # check if the resulting class is correct
    assert isinstance(res, UnoCreator)


def test_create_bot_all_tickers() -> None:
    tickers = (
        LatestPrice.objects.filter(
            ticker__currency_code="HKD",
        )
        .exclude(latest_price=None)
        .values_list(
            "ticker",
            "latest_price",
            named=True,
        )
    )
    tickers_list = list(tickers)

    for item in tickers_list:

        props = BotCreateProps(
            ticker=item.ticker,
            spot_date=datetime.now(),
            investment_amount=100000,
            price=item.latest_price,
            margin=random.randint(1, 2),
            bot_id="UNO_ITM_015384",
        )

        # we start the factory
        factory = BotFactory()
        creator = factory.get_creator(props)
        res = creator.create()

        # get the result
        result: dict = res.get_result_as_dict()

        assert result is not None
        print(f"{item.ticker} is OK")


def test_create_batch_bot() -> None:
    tickers = (
        LatestPrice.objects.filter(
            ticker__currency_code="HKD",
        )
        .exclude(latest_price=None)
        .values_list(
            "ticker",
            "latest_price",
            named=True,
        )
    )
    tickers_list = list(tickers)

    list_props: List[BotCreateProps] = []

    for item in tickers_list:
        props = BotCreateProps(
            ticker=item.ticker,
            spot_date=datetime.now(),
            investment_amount=100000,
            price=item.latest_price,
            margin=random.randint(1, 2),
            bot_id="UNO_ITM_015384",
        )

        list_props.append(props)

    factory = BotFactory()
    creator = factory.get_batch_creator(list_props)
    res = creator.create()

    # get the result
    result: List[dict] = res.get_result_as_dict()

    assert result is not None

    for item in result:
        jsonprint(item)
