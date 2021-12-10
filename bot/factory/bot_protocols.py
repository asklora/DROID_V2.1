from typing import Protocol


class ValidatorProtocol(Protocol):
    def validate(self):
        ...


class EstimatorProtocol(Protocol):
    def calculate(self):
        ...
