import socket
from typing import Union

import pytest
from django.conf import settings
from django.test.client import Client
from dotenv import load_dotenv
from environs import Env
from firebase_admin import firestore

from core.djangomodule.network.cloud import DroidDb
from core.user.models import Accountbalance, TransactionHistory, User

from tests.utils import delete_user

load_dotenv()
env = Env()


@pytest.fixture(scope="session")
def django_db_setup():
    db = DroidDb()
    if settings.DEBUG:
        read_endpoint, write_endpoint, port = db.test_url
    else:
        read_endpoint, write_endpoint, port = db.prod_url

    DB_ENGINE = "django.db.backends.postgresql_psycopg2"
    settings.DATABASES["default"] = {
        "ENGINE": DB_ENGINE,
        "HOST": write_endpoint,
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ml2021#LORA",
        "PORT": port,
    }
    settings.DATABASES["aurora_read"] = {
        "ENGINE": DB_ENGINE,
        "HOST": read_endpoint,
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ml2021#LORA",
        "PORT": port,
    }
    settings.DATABASES["aurora_write"] = {
        "ENGINE": DB_ENGINE,
        "HOST": write_endpoint,
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "ml2021#LORA",
        "PORT": port,
    }


@pytest.fixture(scope="session")
def user(django_db_setup, django_db_blocker):
    # Creating unique user for each computer and invocation
    computer_name = socket.gethostname().lower()
    unique_email = f"{computer_name}@tests.com"

    with django_db_blocker.unblock():
        user = User.objects.create_user(
            email=unique_email,
            username=computer_name,
            first_name="Test",
            last_name="on " + computer_name,
            gender="other",
            phone="012345678",
            password="everything_is_but_a_test",
            is_active=True,
            current_status="verified",
            # is_test=True
        )
        user_balance = Accountbalance.objects.create(
            user=user,
            amount=0,
            currency_code_id="HKD",
        )
        TransactionHistory.objects.create(
            balance_uid=user_balance,
            side="credit",
            amount=200000,
            transaction_detail={"event": "first deposit"},
        )

        yield user

        # clean up
        delete_user(user)


@pytest.fixture(scope="session")
def client() -> Client:
    return Client(raise_request_exception=True)


@pytest.fixture(scope="session")
def firestore_client() -> Client:
    return firestore.client()


@pytest.fixture(scope="session")
def authentication(client, user) -> Union[dict, None]:
    response = client.post(
        path="/api/auth/",
        data={
            "email": user.email,
            "password": "everything_is_but_a_test",
        },
    )

    if (
        response.status_code != 200
        or response.headers["Content-Type"] != "application/json"
    ):
        return None

    response_body = response.json()
    return {"HTTP_AUTHORIZATION": "Bearer " + response_body["access"]}


@pytest.fixture
def order(authentication, client, user) -> Union[dict, None]:
    data = {
        "ticker": "3377.HK",
        "price": 1.63,
        "bot_id": "STOCK_stock_0",
        "amount": 100,
        "margin": 1,
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
        return None

    return response.json()
