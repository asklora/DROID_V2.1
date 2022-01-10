from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Union

from django.utils import timezone


# Base check class
@dataclass
class Check(ABC):
    check_key: str
    data: Any = field(init=False)
    result: dict = field(default_factory=dict, init=False)

    @abstractmethod
    def execute(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_result(self) -> dict:
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError


@dataclass
class Market:
    name: str
    currency: str
    fin_id: str


@dataclass
class Endpoint:
    name: str
    url: str


# Main healthcheck class
# It is also a Check class, meaning every Check class can be run
# individually or in unison using this one
@dataclass
class HealthCheck(Check):
    check_key: str = "healthcheck"
    checks: List[Check] = field(default_factory=list)

    def _get_timestamp(self) -> str:
        return timezone.now().strftime("%A, %d %B %Y")

    def execute(self) -> bool:
        failure: List = []

        for check in self.checks:
            if not check.execute():
                failure.append(check.check_key)

        return True if not failure else False

    def get_result(self) -> dict:
        self.result: dict[str, Union[dict, str]] = {
            "date": timezone.now().isoformat(),
        }
        for check in self.checks:
            self.result[check.check_key] = check.get_result()

        return self.result

    def __str__(self) -> str:
        if not self.result:
            self.get_result()

        result: str = "Healthcheck for "
        result += timezone.now().strftime("%A, %d %B %Y")
        for check in self.checks:
            result += str(check)

        return result
