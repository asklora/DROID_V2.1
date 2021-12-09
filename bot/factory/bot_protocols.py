

from typing import Protocol




class ValidatorProtocol(Protocol):
    
    def validate(self):
        ...

class EstimatorProtocol(Protocol):
    
    def calculate(self):
        ...

class BotCreatorProtocol(Protocol):
    
    def create(self):
        ...

class BotHedgerProtocol(Protocol):
    
    def hedge(self):
        ...
        

class BotStopperProtocol(Protocol):
    
    def stop(self):
        ...
    
    