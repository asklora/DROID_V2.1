from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List

from django.utils import timezone


# Base check class
class Check(ABC):
    data: Any = None
    error: str = ""

    @abstractmethod
    def get_result(self) -> str:
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

    def execute(self) -> str:
        self.result: str = "Healthcheck for "
        self.result += timezone.now().strftime("%A, %d %B %Y")
        for check in self.checks:
            self.result += check.get_result()

        return self.result
