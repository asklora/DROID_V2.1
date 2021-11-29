import pytest
from core.services.healthcheck.run import run_healthcheck
from django.utils import timezone

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_healthcheck(mocker) -> None:
    def send_report(result: str):
        today: str = timezone.now().strftime("%A, %d %B %Y")

        print(result)

        assert result is not None
        assert today in result

    patch = mocker.patch(
        "core.services.healthcheck.run.send_report",
        wraps=send_report,
    )
    mocker.patch(
        "core.services.healthcheck.checks.AskloraCheck.get_celery_result",
        return_value={
            "date": "2021-11-29",
            "api_status": {"production": "up", "staging": "up"},
            "users_num": '111',
        },
    )

    run_healthcheck.apply()

    patch.assert_called()


def test_healthcheck_error_celery(mocker) -> None:
    def send_report(result: str):
        today: str = timezone.now().strftime("%A, %d %B %Y")

        print(result)

        assert result is not None
        assert today in result

    patch = mocker.patch(
        "core.services.healthcheck.run.send_report",
        wraps=send_report,
    )
    mocker.patch(
        "core.services.healthcheck.checks.AskloraCheck.get_celery_result",
        return_value={
            "err": "check_asklora() takes 0 positional arguments but 1 was given",
        },
    )

    run_healthcheck.apply()

    patch.assert_called()
