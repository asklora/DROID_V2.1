import pytest
from django.conf import settings
from django.test.client import Client

from core.djangomodule.network.cloud import DroidDb


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


@pytest.fixture
def client():
    return Client(raise_request_exception=True)
