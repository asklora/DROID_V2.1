import os

from datetime import datetime, timedelta
from django.utils import timezone

import pytest
from core.djangomodule.calendar import TradingHours

from core.services.exchange_services import market_task_checker
from core.universe.models import ExchangeMarket

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_market_opening_check(mocker) -> None:
    def mock_worker_update() -> None:
        exchange = ExchangeMarket.objects.get(currency_code="USD")
        market = TradingHours(mic=exchange.mic)
        market.run_market_check()

    # we mock the worker restartion
    worker_restarted = mocker.patch(
        "core.services.exchange_services.restart_worker",
        wraps=mock_worker_update,
    )

    # we turn off slack report
    os.environ["USE_SLACK"] = 'False'

    # we pick one exchange
    usd_exchange: ExchangeMarket = ExchangeMarket.objects.get(currency_code="USD")

    # we check if the saved data is correct
    if usd_exchange.until_time > timezone.now():
        # and we change the time so it triggers worker restart
        usd_exchange.until_time = usd_exchange.until_time - timedelta(days=1)
        usd_exchange.save()

    # we run the function here
    market_task_checker.apply()

    # we confirm if the function will restart the worker
    worker_restarted.assert_called()

    # we confirm if the data is correct
    # we need to get the exchange again to see the changes
    usd_exchange = ExchangeMarket.objects.get(currency_code="USD")
    assert usd_exchange.until_time > timezone.now()
