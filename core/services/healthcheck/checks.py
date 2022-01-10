import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, List, Union

import requests
from celery.result import AsyncResult
from config.celery import app
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.services.order_services import pending_order_checker
from core.universe.models import ExchangeMarket
from core.user.models import Accountbalance, TransactionHistory
from django.utils.timezone import now
from schema import Schema, SchemaError

from .base import Check, Endpoint, Market


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
        retry_count = 0

        while celery_result.state == "PENDING":
            time.sleep(2)
            retry_count += 1
            if retry_count > 14:
                break

        result: Any = celery_result.result
        print(result)

        if result is None:
            return None

        return result.get("data", None)

    def execute(self) -> bool:
        payload = {
            "type": "function",
            "module": self.module,
            "payload": self.payload,
        }
        self.data = self.get_celery_result(payload)

        return (
            True
            if self.data is not None
            and all(
                [
                    value == "up"
                    for value in self.data.get("api_status", {}).values()
                ]
            )
            else False
        )

    def get_result(self) -> dict:
        if self.data and {"date", "api_status", "users_num"} <= set(self.data):
            date, api_status, _ = self.data.values()
            todays_date: str = str(now().date())

            datetime_matched: bool = date == todays_date
            api_is_up: bool = all(
                [value == "up" for value in api_status.values()]
            )

            result: dict = {
                "datetime_matched": True if datetime_matched else False
            }

            for key, value in api_status.items():
                result[f"api_check_{key}"] = value

            status: str = "ok" if datetime_matched and api_is_up else "error"

            return {"status": status, "result": result}

        else:
            return {"status": "error"}

    def __str__(self) -> str:
        result: str = ""
        if self.data:
            result = "\n- Celery is working properly"

            if {"date", "api_status", "users_num"} <= set(self.data):
                date, api_status, _ = self.data.values()

                today_date: str = str(now().date())
                # droid_users: int = len(User.objects.all())

                date_match: str = (
                    "in sync for both endpoints"
                    if today_date == date
                    else "*out of sync* :warning:"
                )

                # users_match: str = (
                #     "in sync "
                #     if droid_users == int(users_nums)
                #     else "*out of sync* :warning: "
                # )
                # users_match += (
                #     f"(there are {users_nums} users in asklora database "
                #     f"and {droid_users} users in droid database)"
                # )

                result += f"\n- datetime are {date_match}"
                # result += f"\n- Users data in the db is {users_match}"

                for key, value in api_status.items():
                    status: str = value if "up" else f"*{value}* :warning:"
                    result += f"\n- asklora {key} API is {status}"
            else:
                result += "\n- asklora healtcheck is *not active* :warning:"
        else:
            result = "\n- Celery is *not working properly* :warning:"

        return result


@dataclass
class ApiCheck(Check):
    endpoints: List[Endpoint]

    def get_api_name(self, name: str) -> str:
        return name.replace(" ", "_")

    def execute(self) -> bool:
        for api in self.endpoints:
            name: str = self.get_api_name(api.name)
            try:
                response = requests.head(api.url)
                self.result[name] = (
                    "up" if response.status_code == 200 else "down"
                )
            except Exception as e:
                self.error = str(e)
                self.result[name] = "down"

        return self.is_ok()

    def is_ok(self) -> bool:
        return all([value == "up" for value in self.result.values()])

    def get_result(self) -> dict:
        return {
            "status": "ok" if self.is_ok() else "error",
            "result": self.result,
        }

    def __str__(self) -> str:
        result: str = ""
        for api in self.endpoints:
            name: str = self.get_api_name(api.name)
            is_up: bool = self.result.get(name) == "up"
            status: str = "up" if is_up else "*down* :warning:"
            result += f"\n- {api.name} API is {status}"
        return result


@dataclass
class FirebaseCheck(Check):
    firebase_app: Any
    model: Any
    collection: str
    schema: Schema

    def get_documents(self) -> List[dict]:
        collections = self.firebase_app.collection(self.collection).stream()
        documents: List[dict] = []
        for document in collections:
            document_data: dict = document.to_dict()
            document_data["firebase_id"] = document.id
            documents.append(document_data)
        return documents

    def log_error(self, error: SchemaError, item: dict) -> None:
        item_id: Union[str, None] = item.get("firebase_id", None)
        logging.warning(
            f"document {item_id} in {self.collection} has scheme mismatch",
        )
        print(str(error))

    def check_database(self, item: dict):
        """
        Check if the item is not in the database
        and delete it from firebase
        """
        item_id: Union[str, None] = item.get("firebase_id", None)
        if not self.model.objects.filter(pk=item_id).exists():
            logging.warning(f"{item_id} does not exist in the database")
            self.firebase_app.collection(self.collection).document(
                item_id,
            ).delete()

    def execute(self) -> bool:
        total: int = 0
        success: int = 0
        failed: int = 0
        failed_ids: List[str] = []

        for doc in self.get_documents():
            total += 1
            try:
                document_data: dict = doc.copy()
                del document_data["firebase_id"]
                self.schema.validate(document_data)
                success += 1
            except SchemaError as e:
                failed += 1
                failed_ids.append(str(doc.get("firebase_id")))
                self.log_error(error=e, item=doc)
                self.check_database(item=doc)

        self.result = {
            "status": "ok" if failed == 0 else "error",
            "total": total,
            "success": success,
            "failed": failed,
            "failed_ids": failed_ids,
        }

        return True if failed == 0 else False

    def get_result(self) -> dict:
        return self.result

    def __str__(self) -> str:
        result: str = f"\n- `{self.collection}` data in Firebase "
        status: str = ""
        info: str = (
            f"{self.result.get('total', 0)} checked, "
            f"{self.result.get('success', 0)} success and "
            f"{self.result.get('failed', 0)} failed"
        )

        if self.result.get("failed", 0) > 0:
            errors: str = ", ".join(self.result.get("failed_id", []))
            status = f"*has schema mismatch* :warning: {errors}"
        else:
            status = "is correct"

        result += f"{status} ({info})"

        return result


@dataclass
class TestUsersCheck(Check):
    firebase_app: Any
    model: Any
    collection: str

    def get_firebase_ref(self) -> Any:
        return (
            self.firebase_app.collection(self.collection)
            .where("profile.first_name", "==", "Test")
            .stream()
        )

    def get_database_objects(self) -> Any:
        return self.model.objects.filter(
            first_name="Test",
            email__iendswith="@tests.com",
        )

    def list_firebase_test_users(self) -> None:
        collections = self.get_firebase_ref()
        users: List[str] = []
        for document in collections:
            users.append(document.id)
        self.result["firebase"] = {"users": users}

    def list_database_test_users(self) -> None:
        users = [str(user.pk) for user in self.get_database_objects()]
        self.result["database"] = {"users": users}

    def delete_firebase_test_users(self) -> bool:
        batch = self.firebase_app.batch()
        collections = self.get_firebase_ref()

        try:
            for document in collections:
                batch.delete(document.reference)

            batch.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def delete_database_test_users(self) -> bool:
        users: List[Any] = self.get_database_objects()

        try:
            for user in users:
                PositionPerformance.objects.filter(
                    position_uid__user_id=user
                ).delete()
                Order.objects.filter(user_id=user).delete()
                OrderPosition.objects.filter(user_id=user).delete()
                TransactionHistory.objects.filter(
                    balance_uid__user=user
                ).delete()
                Accountbalance.objects.filter(user=user).delete()
                user.delete()

            return True
        except Exception as e:
            print(e)
            return False

    def is_ok(self) -> bool:
        firebase_test_users = self.result.get("firebase", {}).get("users", [])
        database_test_users = self.result.get("database", {}).get("users", [])
        return len(firebase_test_users) == 0 and len(database_test_users) == 0

    def execute(self) -> bool:
        self.list_firebase_test_users()
        self.list_database_test_users()

        firebase_test_users = self.result.get("firebase", {}).get("users", [])
        database_test_users = self.result.get("database", {}).get("users", [])

        print(f"firebase test users' IDs: {', '.join(firebase_test_users)}")
        print(f"database test users' IDs: {', '.join(database_test_users)}")

        if firebase_test_users:
            deleted: bool = self.delete_firebase_test_users()
            self.result["firebase"]["deleted"] = deleted

        if database_test_users:
            deleted: bool = self.delete_database_test_users()
            self.result["database"]["deleted"] = deleted

        return self.is_ok()

    def get_result(self) -> dict:
        return {
            "status": "ok" if self.is_ok() else "error",
            "result": self.result,
        }

    def __str__(self) -> str:
        firebase_data = self.result.get("firebase", {})
        database_data = self.result.get("database", {})

        firebase_test_users = firebase_data.get("users", [])
        database_test_users = database_data.get("users", [])

        firebase_test_users_num: int = len(firebase_test_users)
        database_test_users_num: int = len(database_test_users)

        deleted: bool = (
            # there were test users in firebase
            firebase_data.get("deleted", False)
            # or in database
            or database_data.get("deleted", False)
        )

        firebase_num: str = (
            f"*{firebase_test_users_num}*"
            if firebase_test_users_num > 0
            else str(firebase_test_users_num)
        )
        database_num: str = (
            f"*{database_test_users_num}*"
            if database_test_users_num > 0
            else str(database_test_users_num)
        )

        result: str = (
            "\n- "
            + f"There {'were ' if deleted else 'are '}"
            + firebase_num
            + " test users in Firebase, and "
            + database_num
            + " test users in the database"
            + f"{' (now deleted)' if deleted else ''}"
        )

        return result


@dataclass
class MarketCheck(Check):
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

    def sync_market(self, market: Market, api, db) -> bool:
        status: bool = False

        if api == "open" and db == "closed":
            try:
                exchange_market: ExchangeMarket = ExchangeMarket.objects.get(
                    fin_id=market.fin_id
                )
                exchange_market.is_open = True
                exchange_market.save()

                pending_order_checker.apply(args=(market.currency))
                status = True
            except ExchangeMarket.DoesNotExist:
                print("Market not found")

        return status

    def is_ok(self) -> bool:
        return all(
            [value.get("status") == "ok" for value in self.result.values()]
        )

    def execute(self) -> bool:
        api_result: dict = self.check_market()
        db_result: dict = self.check_database()

        for market in self.markets:
            market_name: str = market.name.lower().replace(" ", "_")
            status_api = api_result.get(market.fin_id)
            status_db = db_result.get(market.fin_id)

            self.result[market_name] = {
                "status": "ok" if status_api == status_db else "error",
                "api": status_api,
                "db": status_db,
                "fixed": False,
            }

            if status_api != status_db:
                fixed: bool = self.sync_market(
                    market=market,
                    api=status_api,
                    db=status_db,
                )
                self.result[market_name]["fixed"] = fixed

        return True if self.is_ok() else False

    def get_result(self) -> dict:
        status: str = "ok" if self.is_ok() else "error"
        return {"status": status, "result": self.result}

    def __str__(self) -> str:
        result: str = ""

        for market in self.markets:
            market_name: str = market.name.lower().replace(" ", "_")
            data: dict = self.result.get(market_name, {})

            result += f"\n- {market.name} is "

            if data.get("status") == "ok":
                result += f"in sync ({data.get('api')})"
            else:
                result += "*out of sync* :warning:, "
                result += f"TradingHours: {data.get('api')}, "
                result += f"database: {data.get('db')}"

                if data.get("fixed"):
                    result += " (we have fixed the issue for now)"

        return result


@dataclass
class TestProjectCheck(Check):
    check_key: str
    api_key: str
    project_id: str
    job_id: str

    def execute(self) -> bool:
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

        return True if self.data.get("status") == "success" else False

    def get_result(self) -> dict:
        return {
            "status": {
                "date": self.data.get("date"),
                "result": self.data.get("status"),
            }
        }

    def __str__(self) -> str:
        result: str = ""
        if self.data:
            result += "\n- Latest TestProject test is "
            result += f"*{self.data['status'].lower()}*"
        else:
            result += "\n- *No* TestProject test runs yet today"
        return result
