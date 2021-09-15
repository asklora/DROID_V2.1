from typing import List

import pytest
from bot.calculate_bot import check_date, get_expiry_date
from core.bot.models import BotOptionType

from utils import create_buy_order

pytestmark = pytest.mark.django_db


def test_should_confirm_bot_expiry_for_classic(user) -> None:
    bot_types: List[BotOptionType] = BotOptionType.objects.filter(
        bot_type="CLASSIC"
    ).order_by("time_to_exp")

    print("\nbot types: " + str(len(bot_types)))

    for bot_type in bot_types:
        order = create_buy_order(
            bot_id=bot_type.bot_id,
            price=15.36,
            ticker="9901.HK",
            user_id=user.id,
            margin=2,
        )

        expiry = get_expiry_date(
            bot_type.time_to_exp,
            order.created,
            order.ticker.currency_code.currency_code,
            apps=True,
        )
        expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

        print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
        print("Duration: " + bot_type.duration)
        print("Expiry date in setup: " + order.setup["position"]["expiry"])
        print("Calculated expiry date: " + expiry_date)

        assert order.setup["position"]["expiry"] == expiry_date


def test_should_confirm_bot_expiry_for_uno(user) -> None:
    bot_types: List[BotOptionType] = BotOptionType.objects.filter(
        bot_type="UNO"
    ).order_by("time_to_exp")

    print("\nbot types: " + str(len(bot_types)))

    for bot_type in bot_types:
        order = create_buy_order(
            bot_id=bot_type.bot_id,
            price=15.36,
            ticker="9901.HK",
            user_id=user.id,
            margin=2,
        )

        expiry = get_expiry_date(
            bot_type.time_to_exp,
            order.created,
            order.ticker.currency_code.currency_code,
            apps=True,
        )
        expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

        print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
        print("Duration: " + bot_type.duration)
        print("Expiry date in setup: " + order.setup["position"]["expiry"])
        print("Calculated expiry date: " + expiry_date)

        assert order.setup["position"]["expiry"] == expiry_date


def test_should_confirm_bot_expiry_for_ucdc(user) -> None:
    bot_types: List[BotOptionType] = BotOptionType.objects.filter(
        bot_type="UCDC"
    ).order_by("time_to_exp")

    print("\nbot types: " + str(len(bot_types)))

    for bot_type in bot_types:
        order = create_buy_order(
            bot_id=bot_type.bot_id,
            price=15.36,
            ticker="9901.HK",
            user_id=user.id,
            margin=2,
        )

        expiry = get_expiry_date(
            bot_type.time_to_exp,
            order.created,
            order.ticker.currency_code.currency_code,
            apps=True,
        )
        expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

        print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
        print("Duration: " + bot_type.duration)
        print("Expiry date in setup: " + order.setup["position"]["expiry"])
        print("Calculated expiry date: " + expiry_date)

        assert order.setup["position"]["expiry"] == expiry_date
