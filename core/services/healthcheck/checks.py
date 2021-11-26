import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, List

import requests
from core.services.order_services import pending_order_checker
from core.universe.models import ExchangeMarket
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
