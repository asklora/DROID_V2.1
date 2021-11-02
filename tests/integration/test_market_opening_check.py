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
        print('function is mocked')

    worker_restarted = mocker.patch(
        "core.services.exchange_services.restart_worker",
         wraps=mock_worker_update,
    )

    mocker.patch(
        "core.services.exchange_services.update_due",
        return_value=True,  # bisa diganti logic sendiri
    )

    market_task_checker.apply()

    worker_restarted.assert_called()
