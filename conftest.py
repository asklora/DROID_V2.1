import pytest
from django.conf import settings
from django.test.client import Client

from core.djangomodule.network.cloud import DroidDb
from core.user.models import Accountbalance, TransactionHistory, User


@pytest.fixture(scope="session")
def django_db_setup():
    db = DroidDb()
    read_endpoint, write_endpoint, port = db.test_url

    DB_ENGINE = "psqlextra.backend"
    settings.DATABASES["default"] = {
        "ENGINE": DB_ENGINE,
        "HOST": write_endpoint,
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ml2021#LORA",
        "PORT": port,
    }


@pytest.fixture(scope="session")
def user(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        user = User.objects.create_user(
            email="pytest@tests.com",
            username="pikachu_icikiwiw",
            password="helloworld",
            is_active=True,
            current_status="verified",
        )
        user_balance = Accountbalance.objects.create(
            user=user,
            amount=0,
            currency_code_id="HKD",
        )
        trans = TransactionHistory.objects.create(
            balance_uid=user_balance,
            side="credit",
            amount=200000,
            transaction_detail={"event": "first deposit"},
        )
        yield user
        user.delete()


@pytest.fixture
def client():
    return Client(raise_request_exception=True)
