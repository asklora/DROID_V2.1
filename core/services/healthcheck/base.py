from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Tuple, Union

from django.utils import timezone


# Base check class
class Check(ABC):
    check_key: str
    data: Any
    error: str
    result: dict = {}
    result_str: str

    @abstractmethod
    def execute(self) -> bool:
        pass

    @abstractmethod
    def get_result(self) -> str:
        pass

    @abstractmethod
    def get_result_as_dict(self) -> dict:
        pass


@dataclass
class Market:
    name: str
    currency: str
    fin_id: str


# Main healthcheck class
@dataclass
class HealthCheck:
    checks: List[Check] = field(default_factory=list)

    def _get_timestamp(self) -> str:
        return timezone.now().strftime("%A, %d %B %Y")

    def execute(self) -> Tuple[List, List]:
        failure: List = []
        success: List = []

        for check in self.checks:
            if check.execute():
                success.append(check.check_key)
            else:
                failure.append(check.check_key)

        return success, failure

    def get_result_as_dict(self) -> dict:
        result: dict[str, Union[dict, str]] = {
            "date": timezone.now().isoformat(),
        }
        for check in self.checks:
            result[check.check_key] = check.get_result_as_dict()

        return result

    def get_result(self) -> str:
        self.result: str = "Healthcheck for "
        self.result += timezone.now().strftime("%A, %d %B %Y")
        for check in self.checks:
            self.result += check.get_result()

        return self.result
