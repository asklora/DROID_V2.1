from typing import List

import pytest
from bot.calculate_bot import check_date, get_expiry_date
from core.bot.models import BotOptionType

from utils import create_buy_order

@pytest.mark.django_db
class TestBotExpiry:
    def test_should_confirm_bot_expiry_for_classic(self) -> None:
        bot_types: List[BotOptionType] = BotOptionType.objects.filter(
            bot_type="CLASSIC"
        ).order_by("time_to_exp")

        for bot_type in bot_types:
            order = create_buy_order(
                bot_id=bot_type.bot_id,
                price=1317,
                ticker="3377.HK",
                user_id=197,
            )

            expiry = get_expiry_date(
                bot_type.time_to_exp,
                order.created,
                order.ticker.currency_code.currency_code,
            )
            expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

            print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
            print("Expiry date: " + order.setup["position"]["expiry"])
            print("Calculated expiry date: " + expiry_date)
            print("Duration: " + bot_type.duration)

            assert order.setup["position"]["expiry"] == expiry_date

    def test_should_confirm_bot_expiry_for_uno(self) -> None:
        bot_types: List[BotOptionType] = BotOptionType.objects.filter(
            bot_type="UNO"
        ).order_by("time_to_exp")

        for bot_type in bot_types:
            order = create_buy_order(
                bot_id=bot_type.bot_id,
                price=1317,
                ticker="3377.HK",
                user_id=197,
            )

            expiry = get_expiry_date(
                bot_type.time_to_exp,
                order.created,
                order.ticker.currency_code.currency_code,
            )
            expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

            print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
            print("Expiry date: " + order.setup["position"]["expiry"])
            print("Calculated expiry date: " + expiry_date)
            print("Duration: " + bot_type.duration)

            assert order.setup["position"]["expiry"] == expiry_date

    def test_should_confirm_bot_expiry_for_ucdc(self) -> None:
        bot_types: List[BotOptionType] = BotOptionType.objects.filter(
            bot_type="UCDC"
        ).order_by("time_to_exp")

        for bot_type in bot_types:
            order = create_buy_order(
                bot_id=bot_type.bot_id,
                price=1317,
                ticker="3377.HK",
                user_id=197,
            )

            expiry = get_expiry_date(
                bot_type.time_to_exp,
                order.created,
                order.ticker.currency_code.currency_code,
            )
            expiry_date = check_date(expiry).date().strftime("%Y-%m-%d")

            print("\nCreated date: " + order.created.date().strftime("%Y-%m-%d"))
            print("Expiry date: " + order.setup["position"]["expiry"])
            print("Calculated expiry date: " + expiry_date)
            print("Duration: " + bot_type.duration)

            assert order.setup["position"]["expiry"] == expiry_date
