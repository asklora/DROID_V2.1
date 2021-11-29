import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, List, Union

import requests
from celery.result import AsyncResult
from config.celery import app
from core.services.order_services import pending_order_checker
from core.universe.models import ExchangeMarket
from core.user.models import User
from django.utils.timezone import now
from schema import Schema, SchemaError

from .base import Check, Market


@dataclass
class ApiCheck(Check):
    name: str
    url: str

    def __post_init__(self):
        try:
            response = requests.head(self.url)
            if response.status_code == 200:
                self.data = "up"
            else:
                self.data = "*down* :warning:"
        except Exception as e:
            self.error = str(e)

    def get_result(self) -> str:
        return f"\n- {self.name} API is {self.data}"


@dataclass
class FirebaseCheck(Check):
    database: Any
    collection: str
    id_field: str
    schema: Schema
    failed_checks: List[str] = field(default_factory=list)

    def get_documents(self) -> List[dict]:
        collections = self.database.collection(self.collection).stream()
        documents: List[dict] = []
        for document in collections:
            documents.append(document.to_dict())
        return documents

    def __post_init__(self):
        total: int = 0
        success: int = 0
        failed: int = 0

        for doc in self.get_documents():
            total += 1
            try:
                self.schema.validate(doc)
                success += 1
            except SchemaError as e:
                failed += 1
                error: dict = {
                    "ticker": doc.get(self.id_field),
                    "error": e,
                }
                logging.warning(f"{self.collection} scheme mismatch")
                print(error)
                self.error += str(error)
                self.failed_checks.append(str(doc.get(self.id_field)))

        self.data = f"{total} checked, {success} success and {failed} failed"

    def get_result(self) -> str:
        status: str = ""
        if self.failed_checks:
            errors: str = ", ".join(self.failed_checks)
            status += f"*has schema mismatch* :warning:: {errors}"
        else:
            status += "is correct"

        result: str = f"\n- `{self.collection}` data in Firebase "
        result += f"{status} ({self.data})"

        return result


@dataclass
class MarketCheck(Check):
    """
    Since it involves calling an API, I decided it's better to run the check
    for all markets at once, to minimize API calls.
    """

    tradinghours_token: str
    markets: List[Market] = field(default_factory=list)

    def check_market(self) -> dict:
        """get data from TradingHours"""
        status: dict = {}
        fins: List[str] = [market.fin_id for market in self.markets]
        url: str = (
            "http://api.tradinghours.com/v3/markets/status?fin_id="
            + ",".join(fins)
            + "&token="
            + self.tradinghours_token
        )
        response = requests.get(url)
        if response.status_code == 200:
            response_body = response.json()
            for fin in fins:
                status[fin] = response_body["data"][fin]["status"].lower()

        return status

    def check_database(self) -> dict:
        """returns the market status from database"""
        status: dict = {}
        for market in self.markets:
            fin_id = market.fin_id
            status[fin_id] = (
                "open"
                if ExchangeMarket.objects.get(fin_id=fin_id).is_open
                else "closed"
            )
        return status

    def sync_market(self, market: Market, api, db):
        if api == "open" and db == "closed":
            try:
                exchange_market: ExchangeMarket = ExchangeMarket.objects.get(
                    fin_id=market.fin_id
                )
                exchange_market.is_open = True
                exchange_market.save()

                pending_order_checker.apply(args=(market.currency))

                self.result += " (we have fixed the issue for now)"
            except ExchangeMarket.DoesNotExist:
                print("Market not found")

    def __post_init__(self):
        api_result: dict = self.check_market()
        db_result: dict = self.check_database()

        result: str = ""

        for market in self.markets:
            status_api = api_result.get(market.fin_id)
            status_db = db_result.get(market.fin_id)

            result += f"\n- {market.name} is "

            if status_api == status_db:
                result += f"in sync ({status_api})"
            else:
                result += "*out of sync* :warning:, "
                result += f"TradingHours: {status_api}, "
                result += f"database: {status_db}"

                self.sync_market(
                    market=market,
                    api=status_api,
                    db=status_db,
                )

        self.result = result

    def get_result(self) -> str:
        return self.result


@dataclass
class TestProjectCheck(Check):
    api_key: str
    project_id: str
    job_id: str

    def __post_init__(self):
        self.data = {}
        url: str = (
            "https://api.testproject.io/v2/projects/"
            + self.project_id
            + "/jobs/"
            + self.job_id
            + "/reports/latest?format=TestProject"
        )
        response = requests.get(url, headers={"Authorization": self.api_key})
        if response.status_code == 200:
            response_body = response.json()

            latest_test_date: datetime = datetime.strptime(
                response_body["executionEnd"].split(".")[0],
                "%Y-%m-%dT%H:%M:%S",
            )

            if latest_test_date.date() == date.today():
                self.data["date"] = latest_test_date
                self.data["status"] = response_body["testResults"][0]["result"]

    def get_result(self) -> str:
        result: str = ""
        if self.data:
            result += "\n- Latest TestProject test is "
            result += f"*{self.data['status'].lower()}*"
        else:
            result += "\n- No TestProject test runs yet today"
        return result


@dataclass
class AskloraCheck(Check):
    module: str
    payload: Union[dict, None]
    queue: str

    def get_celery_result(
        self,
        payload: Union[dict, None],
    ) -> Union[dict, None]:
        task = app.send_task(
            "config.celery.listener",
            args=(payload,),
            queue=self.queue,
        )
        celery_result = AsyncResult(task.id, app=app)

        while celery_result.state == "PENDING":
            time.sleep(2)

        result: Any = celery_result.result
        print(result)

        return result

    def __post_init__(self):
        payload = {
            "type": "function",
            "module": self.module,
            "payload": self.payload,
        }
        self.data = self.get_celery_result(payload)

    def get_result(self) -> str:
        result: str = ""
        if self.data:
            result = "\n- Celery is working properly"

            if {"date", "api_status", "users_num"} <= set(self.data):
                today_date: str = str(now().date())
                droid_users: int = len(User.objects.all())

                date, api_status, users_nums = self.data.values()

                date_match: str = (
                    "in sync for both endpoints"
                    if today_date == date
                    else "out of sync :warning:"
                )

                users_match: str = (
                    "in sync "
                    if droid_users == int(users_nums)
                    else "out of sync :warning: "
                )
                users_match += (
                    f"(there are {users_nums} users in asklora database "
                    f"and {droid_users} users in droid database)"
                )

                result += f"\n- timezones are {date_match}"
                result += f"\n- Users data in the db is {users_match}"

                for key, value in api_status.items():
                    result += f"\n- asklora {key} API is {value}"
            else:
                result += "\n- asklora healtcheck is not yet active :warning:"
        else:
            result = "\n- Celery is not working properly :warning:"

        return result
