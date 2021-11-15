
from typing_extensions import Protocol


class ValidatorProtocol(Protocol):
    
    def validate(self):
        ...


class OrderProtocol(Protocol):
    
    def execute(self):
        ...