import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List

import requests
from schema import Schema, SchemaError


# Base check class
@dataclass
class Check(ABC):
    result: str

    @abstractmethod
    def run_check(self) -> None:
        ...


# Checking classes
@dataclass
class ApiCheck(Check):
    name: str
    url: str

    def run_check(self) -> None:
        response = requests.head(self.url)
        status: str = "up" if response.status_code == 200 else "down"
        self.result = f"{self.name} is {status}"


@dataclass
class FirebaseCheck(Check):
    database: Any
    collection: str
    schema: Schema
    id_field: str
    failed_checks: List[str] = field(default_factory=list)

    def get_documents(self):
        collections = self.database.collection(self.collection).stream()
        documents = collections.get()
        return [doc.to_dict() for doc in documents]

    def run_check(self) -> None:
        self.num: int = 0
        for doc in self.get_documents():
            self.num += 1
            try:
                self.schema.validate(doc)
            except SchemaError as e:
                error: dict = {
                    "ticker": doc.get(self.id_field),
                    "error": e,
                }
                logging.warning("Universe scheme mismatch")
                print(error)
                self.failed_checks.append(str(doc.get(self.id_field)))

        self.result = (
            f"{self.collection.capitalize()} in firebase "
            f"*{'is correct' if not self.failed_checks else 'has schema mismatch'}*\n"
        )


@dataclass
class Market:
    currency: str
    fin_id: str
    name: str


@dataclass
class MarketCheck(Check):
    markets: List[Market] = field(default_factory=list)

    def check_market(self):
        """get data from TradingHours"""
        status: dict = {}
        markets: List[str] = [market.fin_id for market in self.markets]
        token: str = "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
        url: str = (
            "http://api.tradinghours.com/v3/markets/status?fin_id="
            + ",".join(markets)
            + "&token="
            + token
        )
        response = requests.get(url)
        if response.status_code == 200:
            response_body = response.json()
            for market in markets:
                status[market] = response_body["data"][market]["status"].lower()
        
        return status
    
    def run_check(self) -> None:
        api_result: dict = self.check_market() 
