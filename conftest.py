import random
import socket
from datetime import timedelta
from random import choice
from typing import List, NamedTuple, Union

import pytest
from django.conf import settings
from django.test.client import Client
from django.utils import timezone
from django.utils.translation import activate
from dotenv import load_dotenv
from environs import Env
from firebase_admin import firestore

from core.djangomodule.network.cloud import DroidDb
from core.master.models import LatestPrice
from core.orders.models import Feature
from core.universe.models import Universe
from core.user.models import (
    Accountbalance,
    TransactionHistory,
    User,
    UserDepositHistory,
)
from general.data_process import get_uid
from general.date_process import dateNow
from tests.utils.user import delete_user

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
    # Creating unique user for each computer
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
            is_test=True,
        )
        user_balance = Accountbalance.objects.create(
            user=user,
            amount=0,
            currency_code_id="HKD",
        )
        transaction = TransactionHistory.objects.create(
            balance_uid=user_balance,
            side="credit",
            amount=200000,
            transaction_detail={"event": "first deposit"},
        )
        UserDepositHistory.objects.create(
            uid=get_uid(user.id, trading_day=dateNow(), replace=True),
            user_id=user,
            trading_day=dateNow(),
            deposit=transaction.amount,
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
def tickers() -> List[NamedTuple]:
    yesterday = timezone.now().date() - timedelta(days=1)
    hkd_active_tickers = [
        ticker.ticker
        for ticker in Universe.objects.filter(
            currency_code="HKD", is_active=True
        )
    ]
    tickers = (
        LatestPrice.objects.filter(
            intraday_date=yesterday,
            ticker__in=hkd_active_tickers,
        )
        .exclude(latest_price=None)
        .values_list(
            "ticker",
            "latest_price",
            named=True,
        )
    )

    tickers_list = list(tickers)
    length = 5 if len(tickers_list) > 5 else len(tickers_list)

    return random.sample(tickers_list, length)


@pytest.fixture
def order(authentication, client, user, tickers) -> Union[dict, None]:
    ticker, price = choice(tickers)

    data = {
        "ticker": ticker,
        "price": price,
        "bot_id": "STOCK_stock_0",
        "amount": 10000,
        "margin": 2,
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


@pytest.fixture(scope="function")
def use_chinese():
    activate("zh-hant")


@pytest.fixture(scope="function")
def same_day_sell_feature():
    feature = Feature.objects.get(name="prevent_instant_sell")

    yield feature

    if not feature.active:
        feature.active = True
        feature.save()
