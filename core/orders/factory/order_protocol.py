
from typing_extensions import Protocol


class ValidatorProtocol(Protocol):
    
    def validate(self):
        ...



class GetPriceProtocol(Protocol):
    
    def get_price(self,ticker:list)->float:
        ...
        
        
class OrderProtocol(Protocol):
    validator = ValidatorProtocol
    getter_price =GetPriceProtocol
    
    def execute(self):
        ...